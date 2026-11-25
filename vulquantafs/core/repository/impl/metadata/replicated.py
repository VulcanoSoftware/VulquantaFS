"""Redundancy for the metadata itself: RAID-1 across metadata stores.

A file system's file bytes already have redundancy (a primary store plus
mirror stores). The metadata had exactly one store: a pinned message
*or* a git repository. This module gives the metadata the same shape the
file stores have: one primary metadata store plus any number of
redundant metadata stores (:class:`ReplicatedMetadataRepository`), any
combination of a pinned message in a specific store's channel (Telegram
or Discord, each its own store), a GitHub repo and a GitLab repo.

Behaviour:

* Every ``push`` writes the primary store and non-batched mirrors
  (at once when ``concurrent_stores`` is set). GitLab mirrors batch
  ordinary pushes on the heal worker, unless needed for durability.
  A mirror that fails
  is logged, marked *stale* and skipped -- the write still succeeds as
  long as one store holds the tree. Later pushes leave the stale store
  alone (a store that just failed may answer slowly -- a rate limit's
  cooldown, a full-tree re-sync over ten thousand files -- and a bulk
  upload must never wait it out), the periodic heal re-writes it.
* ``init`` loads the tree from the primary; when the primary cannot be
  read (store down, repository unreachable) it fails over to the first
  mirror that holds one. A store that holds no document at all counts as
  empty, not as a failure: a fresh installation bootstraps an empty tree
  and pushes it everywhere, while a primary that is merely unreachable
  never gets shadowed by an empty bootstrap.
* A background heal (:mod:`vulquantafs.core.metadata_healing`) re-writes the
  stores that are stale or were skipped, so a store that was down for a
  while is brought back in line once it returns. Until then it keeps
  serving its last consistent copy, which is what a read failover after
  a restart falls back to.

Conflict handling: every push stamps the tree with a revision (see
``TGFSMetadata.revision``) and the load serves the store with the
freshest one -- the primary only by preference when a mirror is not
meaningfully newer, never by force. The heal closes any lag; a store
whose copy was beaten at load is handed the winner's tree like a stale one.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import AsyncIterator, Iterable, List, Optional, Sequence, Set

from vulquantafs.core.model import TGFSDirectory, TGFSFileRef, TGFSMetadata
from vulquantafs.core.repository.interface import IMetaDataRepository
from vulquantafs.errors import (
    MetadataBlobTooLarge,
    MetadataNotInitialized,
    NoPinnedMessage,
    NoStoredMetadata,
    TechnicalError,
)

logger = logging.getLogger(__name__)

# A store that answers "I hold no metadata document yet" is empty, not
# broken. Both blob-store shapes raise one of these on a fresh store.
EMPTY_METADATA_ERRORS = (NoPinnedMessage, NoStoredMetadata)

# A mirror's copy is always written shortly after the primary's (the
# writes run in order) and the clocks behind the revisions are not one
# clock: git stores report their server's commit date, document stores
# the time of the writing process. A mirror only wins the load when it
# is newer by more than this much -- otherwise the primary keeps the
# lead, and calm operation never flips the source of truth.
PRIMARY_FRESHNESS_WINDOW_SECONDS = 60.0
LOAD_HEDGE_SECONDS = 1.0


@dataclass
class MetadataStore:
    """One metadata store inside a replicated group.

    ``key`` identifies the store in logs and in the manager API; it is
    derived from the configuration (``pinned_message@<store>``,
    ``github_repo@<repo>@<branch>``), not from the store object.
    """

    key: str
    repo: IMetaDataRepository
    primary: bool = False


class ReplicatedMetadataRepository(IMetaDataRepository):
    def __init__(
        self, stores: Sequence[MetadataStore], concurrent_stores: bool = False
    ):
        if not stores:
            raise ValueError("a replicated metadata repository needs a store")
        if not stores[0].primary:
            raise ValueError("the first metadata store must be the primary")
        self._stores: List[MetadataStore] = list(stores)
        # False keeps the historical one-store-after-another write order, so
        # a push lasts for the sum of every pending store's effort; True
        # writes all stores at once, so it lasts for the slowest one. The
        # stale/skipped bookkeeping of :meth:`_write` is per store and needs
        # no order, so both spread failures the same way.
        self._concurrent_stores = concurrent_stores
        # Keys of the stores that missed a write and await a re-sync.
        self._stale: Set[str] = set()
        # A dirty bit per mirror, not a queue of individual file changes.
        # Writes registered during a flush set it again for the next cycle.
        self._pending_batches: Set[str] = set()
        self._loaded_from_mirror = False
        # Keys of the stores that cannot hold the current document at all
        # (a pinned copy whose messages are too small), with the reason.
        # Re-checked on every heal: a smaller tree fits again.
        self._skipped: dict[str, str] = {}
        # The boot's first push, once init() has run; see init().
        self._initial_push: Optional[asyncio.Task] = None
        # One write at a time per store: an ordinary push, the journal
        # flush and the heal may all want the same store. Two pushes hold
        # the same shared tree object, so their order is without effect.
        self._store_locks = {store.key: asyncio.Lock() for store in stores}
        super().__init__()
        # Read as a plain attribute by the API layer, so it is copied from
        # the primary instead of forwarded dynamically.
        self.push_writes_stats = stores[0].repo.push_writes_stats

    # -- the shared tree ----------------------------------------------------

    async def get(self) -> TGFSMetadata:
        """The tree this layer serves; loads it when nothing is loaded yet."""
        if self.metadata is not None:
            return self.metadata
        tree = await self._load()
        self.metadata = tree
        return tree

    @property
    def metadata(self) -> Optional[TGFSMetadata]:
        return self._stores[0].repo.metadata

    @metadata.setter
    def metadata(self, value: Optional[TGFSMetadata]) -> None:
        # One tree, shared by every store: the APIs mutate it in place and
        # each store serializes (or incrementally writes) that same object.
        primary = self._stores[0].repo
        if self._loaded_from_mirror:
            # Never attach write-through hooks to the primary that may have
            # been unreachable at boot. Fallback mutations must reach push().
            primary.bind_shared_tree(value)
        else:
            primary.metadata = value
        for store in self._stores[1:]:
            store.repo.bind_shared_tree(primary.metadata)

    # -- loading --------------------------------------------------------------

    async def init(self) -> None:
        tree = await self._load()
        self.metadata = tree
        # The first push runs in the background: the boot served nothing
        # until it completed, so the stores that missed something -- mere
        # mirrors by definition -- lined the whole server up behind their
        # re-sync (measured: past 15 minutes of boot over one stale
        # GitHub mirror that kept losing its ref races). The tree is
        # loaded and readable, that is what serving reads; the heal
        # worker would make this exact write a moment later anyway, and
        # like its retries a failing write logs and leaves the store
        # stale instead of taking the server down.
        self._initial_push = asyncio.create_task(
            self._push_at_startup(), name="metadata-initial-push"
        )

    async def _push_at_startup(self) -> None:
        try:
            # include_stale: the boot's one background write still syncs
            # every store (the load left the losers stale); only the
            # ordinary pushes below skip the stale ones.
            await self._push(include_stale=True)
        except TechnicalError as ex:
            # A tree was loaded, so the file system can serve reads: the
            # stores that could not take the write are marked stale and
            # re-synced by the heal worker. Crashing the boot instead kept
            # the whole server down over one store's outage (a rate limit,
            # a repository that came back slower than the write).
            logger.warning(
                f"Could not write the metadata tree at startup; the heal "
                f"worker will re-sync the failed stores: {ex}"
            )
        except Exception:
            # The task dies with the boot gone otherwise; the heal worker
            # still rewrites the stale stores.
            logger.exception("The startup push of the metadata stores failed")

    async def _load(self) -> TGFSMetadata:
        """The tree to serve: the most recent one across the stores.

        The stores are dated by their revision, so the copy a store
        received while the primary was down wins over the primary's
        older one; git stores date themselves by their branch head
        commit. The primary is preferred only while no other store is
        meaningfully newer, so calm operation never flips the source of
        truth on the split seconds between two stores' clocks.

        Stores that could not serve a tree (failed *or* empty *or* older
        than the winner) are marked stale, so the push that follows and
        the periodic heal write the winning tree to them. When the
        stores can answer with their revision cheaply, only the winner's
        tree is loaded. A failing read is not retried on the startup path.
        """
        candidates: List[tuple[MetadataStore, TGFSMetadata]] = []
        loaded: Set[str] = set()
        empty_store: Optional[MetadataStore] = None
        failures: List[str] = []
        first_error: Optional[Exception] = None

        async def probe(store: MetadataStore) -> tuple[TGFSMetadata, bool]:
            revision = await store.repo.head_revision()
            if revision is None:
                return await store.repo.get(), True
            return TGFSMetadata(dir=TGFSDirectory.root_dir(), revision=revision), False

        def failed(store: MetadataStore, ex: Exception) -> None:
            nonlocal first_error
            self._stale.add(store.key)
            first_error = first_error or ex
            failures.append(f"{store.key}: {ex}")
            logger.warning("Could not read metadata from store %s: %s", store.key, ex)

        results = await asyncio.gather(
            *(probe(store) for store in self._stores), return_exceptions=True
        )
        for store, result in zip(self._stores, results):
            if isinstance(result, EMPTY_METADATA_ERRORS):
                self._stale.add(store.key)
                empty_store = empty_store or store
            elif isinstance(result, Exception):
                failed(store, result)
            elif isinstance(result, BaseException):
                raise result
            else:
                tree, is_loaded = result
                candidates.append((store, tree))
                if is_loaded:
                    loaded.add(store.key)

        failed_dated_read = False
        selected: Optional[tuple[MetadataStore, TGFSMetadata]] = None
        async for candidate, loaded_tree in self._read_candidates(candidates, loaded):
            if isinstance(loaded_tree, EMPTY_METADATA_ERRORS):
                self._stale.add(candidate.key)
                empty_store = empty_store or candidate
            elif isinstance(loaded_tree, Exception):
                failed(candidate, loaded_tree)
                failed_dated_read = True
            else:
                selected = candidate, loaded_tree

        if selected is None:
            if empty_store is not None and not failed_dated_read:
                if failures:
                    logger.warning(
                        f"No metadata store could serve a tree "
                        f"({'; '.join(failures)}); starting from the "
                        f"empty tree of {empty_store.key}"
                    )
                # The selected empty store is the bootstrap source, not
                # a stale copy that needs a full remote reconciliation.
                self._stale.discard(empty_store.key)
                self._loaded_from_mirror = not empty_store.primary
                return TGFSMetadata(dir=TGFSDirectory.root_dir())
            raise TechnicalError(
                "No metadata store could be read: " + "; ".join(failures)
            ) from first_error

        winner, tree = selected
        for store, _ in candidates:
            if store is not winner and store.key not in self._stale:
                self._stale.add(store.key)
                logger.info(
                    f"Metadata store {winner.key} holds the most recent "
                    f"metadata tree; store {store.key} will be re-synced"
                )
        self._loaded_from_mirror = not winner.primary
        return (
            winner.repo.export_for_primary(tree) if self._loaded_from_mirror else tree
        )

    async def _read_candidates(
        self,
        candidates: Sequence[tuple[MetadataStore, TGFSMetadata]],
        loaded: Set[str],
    ) -> AsyncIterator[tuple[MetadataStore, TGFSMetadata | Exception]]:
        """Keep the freshness order, but give a slow load a warm fallback."""
        remaining = list(candidates)
        reads: dict[str, asyncio.Task[TGFSMetadata]] = {}
        try:
            while remaining:
                winner, tree = self._freshest(remaining)
                remaining = [(s, t) for s, t in remaining if s is not winner]
                if winner.key in loaded:
                    yield winner, tree
                    return
                task = reads.get(winner.key)
                if task is None:
                    task = reads[winner.key] = asyncio.create_task(winner.repo.get())
                if remaining and len(reads) == 1:
                    fallback, _ = self._freshest(remaining)
                    if fallback.key not in loaded:
                        done, _ = await asyncio.wait({task}, timeout=LOAD_HEDGE_SECONDS)
                        if not done:
                            reads[fallback.key] = asyncio.create_task(
                                fallback.repo.get()
                            )
                try:
                    tree = await task
                except Exception as ex:
                    yield winner, ex
                else:
                    yield winner, tree
                    return
                finally:
                    reads.pop(winner.key, None)
        finally:
            for task in reads.values():
                task.cancel()
            if reads:
                await asyncio.gather(*reads.values(), return_exceptions=True)

    def _freshest(
        self,
        candidates: Sequence[tuple[MetadataStore, TGFSMetadata]],
    ) -> tuple[MetadataStore, TGFSMetadata]:
        """The most recent tree among the loaded ones, the primary
        preferred on a tie or when no store beats it beyond the window.
        A tree without a revision (a legacy document, or a store that
        could not date itself) ranks below every dated one."""
        primary = self._stores[0]
        primary_tree = next(
            (tree for store, tree in candidates if store is primary), None
        )

        def revision(tree: TGFSMetadata) -> float:
            return tree.revision if tree.revision is not None else 0.0

        # A stable sort keeps the configuration order within equal dates.
        ordered = sorted(candidates, key=lambda pair: revision(pair[1]), reverse=True)
        winner, tree = ordered[0]
        if (
            winner is not primary
            and primary_tree is not None
            and primary_tree.revision is not None
            and revision(primary_tree)
            >= revision(tree) - PRIMARY_FRESHNESS_WINDOW_SECONDS
        ):
            return primary, primary_tree
        return winner, tree

    # -- writing ---------------------------------------------------------------

    async def push(self) -> None:
        await self._push()

    @property
    def supports_upload_journal(self) -> bool:
        return self._stores[0].repo.supports_upload_journal

    async def push_upload_refs(
        self, refs: Sequence[TGFSFileRef], *, full: bool = False
    ) -> None:
        # The outbox drains via the primary; mirror copies continue
        # through the normal (and, for GitLab, batched) replication path.
        primary = self._stores[0]
        async with self._store_locks[primary.key]:
            await primary.repo.push_upload_refs(refs, full=full)

    async def push_removals(self) -> None:
        # The journal flusher may not clear an entry until every store,
        # the stale ones included, holds the tree the removal belongs to.
        await self._push(all_full=True, include_stale=True)
        if stale := self.stale_stores():
            # A stale mirror can be the only readable copy at a later
            # boot. Keep the journal until it has the removals as well.
            raise TechnicalError("Waiting for metadata stores: " + ", ".join(stale))

    async def _push(self, all_full: bool = False, include_stale: bool = False) -> None:
        if not self.metadata:
            raise MetadataNotInitialized()
        self._stamp_revision()

        any_store_ok = False
        errors: List[str] = []
        results: List[tuple[bool, Optional[str]]] = []

        # The primary gets its regular write (a github repo primary has
        # already written incrementally while the tree was mutated); the
        # mirrors always get the full sync, since they may have missed any
        # of those incremental writes while they were down. A primary that
        # could not serve the tree at boot (it was down or empty) gets the
        # full sync too: its regular write is a no-op for a tree store, and
        # everything it missed while it was unavailable has to land before
        # the next mutation goes through incrementally.
        def full_for(store: MetadataStore, index: int) -> bool:
            return all_full or index > 0 or store.key in self._stale

        async def one(store: MetadataStore, index: int) -> tuple[bool, Optional[str]]:
            async with self._store_locks[store.key]:
                return await self._write(store, full=full_for(store, index))

        batched = []
        if not include_stale:
            # A push that retried a store marked stale inline would pay
            # its slowness on every following operation; the heal worker
            # re-syncs the store once it answers again.
            stores = []
            for i, store in enumerate(self._stores):
                if i > 0 and store.repo.batch_mirror_writes:
                    self._pending_batches.add(store.key)
                    batched.append((store, i))
                elif store.key not in self._stale:
                    stores.append((store, i))
        else:
            stores = [(store, i) for i, store in enumerate(self._stores)]

        if self._concurrent_stores and len(stores) > 1:
            results.extend(
                await asyncio.gather(*(one(store, i) for store, i in stores))
            )
        else:
            for store, i in stores:
                results.append(await one(store, i))
        for ok, error in results:
            any_store_ok = any_store_ok or ok
            if error:
                errors.append(error)
        # Deferring a mirror is safe only after another copy took the
        # write. If not, try even a stale GitLab mirror that isn't cooling
        # down; its full sync can be the only durable copy.
        if not any_store_ok:
            for store, i in batched:
                if store.repo.write_in_cooldown():
                    errors.append(f"{store.key}: GitLab rate-limit cooldown active")
                    continue
                ok, error = await one(store, i)
                if ok:
                    any_store_ok = True
                    break
                if error:
                    errors.append(error)
        if not any_store_ok:
            # Not one store holds the tree; the caller has to know. A push
            # that skipped every stale store lands here as well.
            raise TechnicalError(
                "Writing the metadata failed for every store: "
                + ("; ".join(errors) or "every store awaits a re-sync by the heal")
            )

    def _stamp_revision(self) -> None:
        """Date the tree being written, so the stores hold comparable
        revisions at the next load. Document shaped stores serialize it
        into the metadata document; git tree stores date themselves by
        their branch head commit either way."""
        tree = self.metadata
        if tree is None:
            return
        now = time.time()
        tree.revision = now if tree.revision is None else max(tree.revision, now)

    async def _write(
        self, store: MetadataStore, full: bool
    ) -> tuple[bool, Optional[str]]:
        """Write the whole tree to ``store``; reports success and, on
        failure, a short reason. Failures mark the store stale (or
        skipped, when it cannot hold the document at all) for the heal."""
        if store.repo.write_in_cooldown():
            self._stale.add(store.key)
            return False, "GitLab rate-limit cooldown active"
        pending = store.key in self._pending_batches
        self._pending_batches.discard(store.key)
        try:
            if full:
                await store.repo.push_full()
            else:
                await store.repo.push()
        except asyncio.CancelledError:
            if pending:
                self._pending_batches.add(store.key)
            raise
        except MetadataBlobTooLarge as ex:
            reason = str(ex)
            if store.key not in self._skipped:
                logger.warning(
                    f"Metadata store {store.key} cannot hold the metadata "
                    f"document; it is skipped until a smaller document fits: "
                    f"{reason}"
                )
            self._skipped[store.key] = reason
            return False, reason
        except Exception as ex:
            reason = str(ex) or repr(ex)
            self._stale.add(store.key)
            if not store.repo.write_in_cooldown():
                logger.error(
                    f"Writing the metadata to store {store.key} failed; it will "
                    f"be re-synced once it is available again: {reason}"
                )
            return False, reason
        self._stale.discard(store.key)
        self._skipped.pop(store.key, None)
        return True, None

    async def heal(self) -> None:
        """Re-write every store that has a batch pending or is stale/skipped.

        Called periodically by the heal worker and safe to call any time;
        a store that is up to date costs nothing, and one that just came
        back is brought in line with the current tree. Each store's own
        lock keeps this apart from the pushes of the same store, so a
        slow re-sync holds up nothing but that store. Maintenance is
        deliberately separate so history reads don't hold a write lock.
        """
        if self._initial_push is not None and not self._initial_push.done():
            return
        await self._heal()

    async def _heal(self) -> None:
        if not self.metadata:
            return

        async def sync(store: MetadataStore) -> None:
            if store.repo.write_in_cooldown() or self._store_locks[store.key].locked():
                return
            async with self._store_locks[store.key]:
                if (
                    store.key in self._pending_batches
                    or store.key in self._stale
                    or store.key in self._skipped
                ):
                    await self._write(store, full=True)

        await asyncio.gather(*(sync(store) for store in self._stores))

    async def maintain(self) -> None:
        if not self.metadata or (
            self._initial_push is not None and not self._initial_push.done()
        ):
            return
        for store in self._stores:
            if (
                store.key in self._stale
                or store.key in self._skipped
                or store.key in self._pending_batches
                or store.repo.write_in_cooldown()
            ):
                continue
            try:
                await store.repo.maintain()
            except Exception as ex:
                logger.warning(f"Maintaining metadata store {store.key} failed: {ex}")

    def stale_stores(self) -> List[str]:
        return sorted(self._stale | set(self._skipped))

    async def close(self) -> None:
        # Stop the boot push that has not finished, like the coalescer's
        # pusher: a shutdown that races the first re-sync otherwise waits
        # for (or kills) an already doomed task at interpreter teardown.
        task = self._initial_push
        self._initial_push = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        for store in self._stores:
            await store.repo.close()

    # -- file stats (``listing.stat_in_metadata``) ---------------------------
    #
    # Stats are a primary-store concern: they ride on the file refs of the
    # shared tree (blob stores) or live in the primary git repo's stats
    # files (``github_repo``). Mirrors pick them up through the shared tree
    # or not at all.

    async def load_stats(self, directory: TGFSDirectory) -> bool:
        return await self._stores[0].repo.load_stats(directory)

    def stats_stored(self, directory: TGFSDirectory) -> bool:
        return self._stores[0].repo.stats_stored(directory)

    async def save_stats(
        self, directories: Iterable[TGFSDirectory], keep: bool
    ) -> None:
        await self._stores[0].repo.save_stats(directories, keep)

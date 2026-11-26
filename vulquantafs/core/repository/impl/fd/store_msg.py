import asyncio
import inspect
import json
import logging
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

from vulquantafs.backends.base import IStore
from vulquantafs.config import get_config
from vulquantafs.core.mirror import MirrorGroup
from vulquantafs.core.model import TGFSFileDesc, TGFSFileRef, TGFSFileVersion
from vulquantafs.core.model.file import INVALID_FILE_SIZE
from vulquantafs.core.repository.interface import (
    FDRepositoryResp,
    IFDRepository,
)
from vulquantafs.errors import (
    MessageNotFound,
    RpcTimeoutError,
    TechnicalError,
    is_store_outage,
)
from vulquantafs.reqres import MessageResp

logger = logging.getLogger(__name__)

# How long a store the breaker has taken away is still given to answer a
# read. Every read that runs in front of a download's first byte has to
# fit in the 20 s a client waits before dropping an idle transfer, so an
# unreachable store gets a short probe: long enough for a store that is
# merely slow to answer, short enough that the client still sees the
# answer -- usually the file from a mirror, or an error -- in time.
UNREACHABLE_PROBE_TIMEOUT_S = 3.0

# Bound on the "are these parts still there" lookup, which every download
# and listing does before it reads a single byte. It used to be
# unbounded: with the primary gone, opening one file blocked for the whole
# reconnect cycle of the Telegram client, FileZilla abandoned the transfer
# after 20 s, and the retry hit the same wall -- while the mirror that
# held the parts all along waited to be asked. A store that does not
# answer within this window is treated as unavailable, exactly like one
# that raised.
PART_LOOKUP_TIMEOUT_S = 8.0


def _bounded_read(
    what: str, action: Awaitable, timeout: Optional[float] = None
) -> Awaitable:
    """Bound one store read by ``transfer.upload_part_timeout_s``

    The same bound that keeps a silent part upload from freezing an
    SFTP client's write acknowledgements keeps a silent store read
    from freezing the write's own preparation: ``rpc_timeout_s`` is
    minutes, and a client whose read never comes back abandons the
    transfer long before the timeout converts the hang into an error
    the failover can act on. ``0`` keeps reads unbounded. ``timeout``
    overrides the configured bound for reads that run in front of a
    client waiting for its first byte of a download.
    """
    if timeout is None:
        timeout = get_config().vulquantafs.transfer.upload_part_timeout_s
    if not timeout:
        return action

    async def run():
        try:
            return await asyncio.wait_for(action, float(timeout))
        except asyncio.TimeoutError as ex:
            raise RpcTimeoutError(
                f"A store did not answer {what} within {timeout}s"
            ) from ex

    return run()


class StoreFDRepository(IFDRepository):
    """File descriptors as JSON text messages in a store.

    Backend-agnostic; mirror copies of the descriptor are kept by the
    :class:`MirrorGroup` and read back when the primary copy is gone.
    """

    def __init__(
        self,
        store: IStore,
        mirror_group: Optional[MirrorGroup] = None,
        local_versions: Optional[Callable[[str], bool]] = None,
    ):
        self._message_api = store
        self._mirror_group = mirror_group
        # Whether this instance's local cache holds a pending version in
        # full (``write_ack: cache``). Such a version is readable here and
        # nowhere else.
        self._local_versions = local_versions

    def _readable_locally(self, version: TGFSFileVersion) -> bool:
        return (
            version.pending
            and self._local_versions is not None
            and self._local_versions(version.id)
        )

    @staticmethod
    def _accepts(store, message_id: int) -> bool:
        """Whether ``store`` can address ``message_id``.

        Stores outside this package (a test double, an embedding app's own
        implementation) may not carry the capability; they are trusted, the
        way they were before it existed.
        """
        accepts = getattr(store, "accepts_message_id", None)
        if accepts is None:
            return True
        answer = accepts(message_id)
        if inspect.iscoroutine(answer):
            # A double that answers every attribute with a mock coroutine.
            answer.close()
            return True
        return bool(answer)

    def _foreign_store_of(self, message_id: int) -> Optional[str]:
        """The configured store an id can belong to, when this one cannot.

        A descriptor written while another store was the primary records
        its ids without a store -- "the primary", which tgfs always meant.
        An id is only meaningful inside the store that issued it, though:
        a 19-digit Discord snowflake can never name a Telegram message.
        Asking the primary for one used to fail the whole batched request,
        which cost every file read in the same window its descriptor and so
        served it as an empty file, while its recorded size stayed correct.
        """
        if not message_id or self._accepts(self._message_api, message_id):
            return None
        group = self._mirror_group
        if group is None:
            return None
        for key in group.store_keys:
            store = group.store_for(key)
            if store is not None and self._accepts(store, message_id):
                return key
        return None

    def _rehome_ref(self, fr: TGFSFileRef) -> None:
        """Point a ref whose id belongs elsewhere at that store."""
        if not fr.owned_by(self.key):
            return
        key = self._foreign_store_of(fr.message_id)
        if key is not None:
            logger.warning(
                f"File descriptor {fr.message_id} of {fr.name} cannot belong to "
                f"{self.key}; reading it from {key}"
            )
            fr.store = key

    def _rehome_versions(self, fd: TGFSFileDesc) -> None:
        """Record the store that owns versions whose ids this one cannot hold."""
        for version in fd.get_versions():
            if not version.owned_by(self.key) or not version.message_ids:
                continue
            if any(
                self._accepts(self._message_api, mid)
                for mid in version.message_ids
                if mid > 0
            ):
                continue
            holders = {
                key
                for mid in version.message_ids
                if mid > 0
                if (key := self._foreign_store_of(mid)) is not None
            }
            if len(holders) != 1:
                continue
            version.store = holders.pop()
            logger.warning(
                f"Content of {fd.name}@{version.id} cannot live in {self.key}; "
                f"reading it from {version.store}"
            )

    @property
    def key(self) -> str:
        return self._message_api.key

    def _stamp(self, fd: TGFSFileDesc) -> None:
        """Record which store the versions' ids belong to.

        Versions without a store were written by tgfs or duplicated into
        this primary a moment ago; either way their ids are this store's.
        Versions relocated from a former primary keep their store.
        """
        for version in fd.get_versions():
            if version.store is None and version.is_valid():
                version.store = self.key

    async def save(
        self, fd: TGFSFileDesc, fr: Optional[TGFSFileRef] = None
    ) -> FDRepositoryResp:
        self._stamp(fd)

        # A primary the circuit breaker marks dead is skipped outright:
        # the old behaviour (a failing edit_message_text retried against a
        # store that is down) only burns time before the write fails. The
        # acting store writes the descriptor and owns it in its place; the
        # backfill returns both to the primary once it is back.
        group = self._mirror_group
        candidates = group.acting_candidates() if group is not None else []
        if not (group and candidates and group.primary_dead()):
            try:
                return await self._save_into(self._message_api, fd, fr, self.key)
            except Exception as ex:
                if not candidates:
                    raise
                logger.warning(
                    f"Could not write the file descriptor of {fd.name} to the "
                    f"primary store ({ex}); an acting store takes the write"
                )
        last: Optional[Exception] = None
        for ch in candidates:
            try:
                return await self._save_into(ch.store, fd, fr, ch.key, skip=ch.key)
            except Exception as ex:
                last = ex
                logger.warning(
                    f"Could not write the file descriptor of {fd.name} to the "
                    f"acting store {ch.key}: {ex}"
                )
        raise last or TechnicalError(f"No store accepted the descriptor of {fd.name}")

    async def _save_into(
        self,
        store: IStore,
        fd: TGFSFileDesc,
        fr: Optional[TGFSFileRef],
        key: str,
        skip: Optional[str] = None,
    ) -> FDRepositoryResp:
        # If file_content referer is None, create a new file_content descriptor message.
        if fr is None:
            return FDRepositoryResp(
                message_id=await store.send_text(fd.to_json()),
                fd=fd,
                mirrors=await self._mirror_fd(fd, existing=None, skip=skip),
                store=key,
            )

        # A descriptor that lives in a former primary (the mirror was
        # promoted) must not be edited by id here -- the id names some
        # other message in this store. Send a fresh copy and keep the old
        # one as that store's mirror entry. Not the primary's own entry,
        # though: a failed-over save edits the acting store while the
        # primary is down, and its ``mirrors`` entry there is the stale
        # descriptor it already holds. Recording it would let a later
        # read relocate the whole ref to that stale copy.
        if not fr.owned_by(key):
            existing = dict(fr.mirrors)
            if fr.store and fr.store != self.key:
                existing[fr.store] = fr.message_id
            if skip is not None:
                # The acting store's own entry is superseded: its fresh
                # copy becomes the owned descriptor message.
                existing.pop(skip, None)
            return FDRepositoryResp(
                message_id=await store.send_text(fd.to_json()),
                fd=fd,
                mirrors=await self._mirror_fd(fd, existing=existing, skip=skip),
                store=key,
            )

        # If file_content referer is provided, try to update the existing file_content descriptor.
        # But if the message is not found (probably got deleted manually), a new file_content descriptor will be created.
        try:
            return FDRepositoryResp(
                message_id=await store.edit_message_text(
                    message_id=fr.message_id, message=fd.to_json()
                ),
                fd=fd,
                mirrors=await self._mirror_fd(fd, existing=fr.mirrors, skip=skip),
                store=key,
            )
        except MessageNotFound:
            return await self._save_into(store, fd, None, key, skip=skip)

    async def _mirror_fd(
        self,
        fd: TGFSFileDesc,
        existing: Optional[Dict[str, int]],
        skip: Optional[str] = None,
    ) -> Dict[str, int]:
        """Keep a copy of the FD text message in every mirror store.

        Descriptors are sent as fresh text messages (not forwarded)
        because they are edited on every new version and a forwarded
        message cannot be edited in the target channel. With ``skip`` the
        store that holds the descriptor itself is left out: the write
        failover made it the descriptor's own home.
        """
        if not self._mirror_group:
            return dict(existing or {})
        return await self._mirror_group.mirror_fd(fd.to_json(), existing, skip=skip)

    def _fully_mirrored(self, version: TGFSFileVersion) -> bool:
        """Whether every part of ``version`` has a copy in a live mirror.

        Only then can a read skip the store that owns the version: without
        a mirror copy of a single part, that part has nowhere else to come
        from and the owner is still the only one who can answer.
        """
        group = self._mirror_group
        if group is None or not version.message_ids:
            return False
        for j in range(len(version.message_ids)):
            if not any(
                # A version lists its own store among its ``mirrors``; that
                # copy is the one being skipped, not a way around it.
                key != self.key
                and j < len(ids)
                and ids[j] > 0
                and group.store_for(key) is not None
                for key, ids in version.mirrors.items()
            ):
                return False
        return True

    async def _lookup_mirror_parts(
        self, missing: List[Tuple[str, int]]
    ) -> Dict[Tuple[str, int], int]:
        """Fetch mirror copies of content messages, batched per channel.

        ``missing`` is a list of (store_key, message_id) pairs; the
        result maps each found pair to the document size of the copy.
        """
        res: Dict[Tuple[str, int], int] = {}
        if not self._mirror_group:
            return res
        by_channel: Dict[str, List[int]] = {}
        for store_key, mid in missing:
            by_channel.setdefault(store_key, []).append(mid)
        for store_key, mids in by_channel.items():
            if (api := self._mirror_group.store_for(store_key)) is None:
                continue
            try:
                messages = await _bounded_read(
                    f"the mirror part lookup for messages {mids} in {store_key}",
                    api.get_messages(mids),
                )
            except Exception as ex:
                logger.warning(
                    f"Could not check mirror store {store_key} for "
                    f"messages {mids}: {ex}"
                )
                continue
            for mid, message in zip(mids, messages):
                if message and message.document:
                    res[(store_key, mid)] = message.document.size
        return res

    async def _verify_replicas(self, version: TGFSFileVersion) -> bool:
        """Whether some replica of ``version`` is complete; fills in the
        replica's part sizes when they were not recorded."""
        if not self._mirror_group:
            return False
        complete = False
        for store_key, replica in version.replicas.items():
            store = self._mirror_group.store_for(store_key)
            if store is None or not replica.message_ids:
                continue
            try:
                messages = await _bounded_read(
                    f"the replica lookup for version {version.id} in {store_key}",
                    store.get_messages(replica.message_ids),
                )
            except Exception as ex:
                logger.warning(
                    f"Could not check replica in store {store_key} for "
                    f"version {version.id}: {ex}"
                )
                continue
            if any(m is None or m.document is None for m in messages):
                continue
            replica.part_sizes = [m.document.size for m in messages]  # type: ignore[union-attr]
            complete = True
        return complete

    async def _acting_map(
        self, versions: List[TGFSFileVersion]
    ) -> Tuple[Dict[Tuple[str, int], MessageResp], set[str]]:
        """Look up the parts of acting-owned versions in their stores.

        A version a failed-over write owns lives in a live mirror; its
        ids are checked there instead of against a primary that never
        saw the parts (and is likely down). A store that cannot answer
        simply contributes nothing, like a mirror below.
        """
        if self._mirror_group is None:
            return {}, set()
        by_store: Dict[str, List[int]] = {}
        for version in versions:
            if version.owned_by(self.key):
                continue
            if (
                version.store is None
                or version.store not in self._mirror_group.store_keys
            ):
                continue
            by_store.setdefault(version.store, []).extend(
                mid for mid in version.message_ids if mid > 0
            )
        res: Dict[Tuple[str, int], MessageResp] = {}
        unavailable: set[str] = set()
        for store_key, ids in by_store.items():
            api = self._mirror_group.store_for(store_key)
            if api is None:
                continue
            try:
                found = await _bounded_read(
                    f"the part lookup in the acting store {store_key}",
                    api.get_messages(ids),
                )
            except Exception as ex:
                unavailable.add(store_key)
                logger.warning(
                    f"Could not check the acting store {store_key} for the "
                    f"parts it owns: {ex}"
                )
                continue
            res.update({(store_key, m.message_id): m for m in found if m})
        return res, unavailable

    async def _validate_fv(
        self, fd: TGFSFileDesc, include_all_versions: bool
    ) -> TGFSFileDesc:
        versions = fd.get_versions(exclude_invalid=True)

        # Files in the channel may be deleted manually, so we need to check if the messages for the versions exist.
        # Only ids that belong to this store are looked up here; a version
        # relocated from a former primary is checked against the mirrors.

        all_ids = [
            mid
            for version in versions
            if version.owned_by(self.key)
            for mid in version.message_ids
            # An id this store cannot hold is not a missing part: it belongs
            # to another store's namespace and must never be sent here.
            if self._accepts(self._message_api, mid)
        ]
        primary_down = (
            self._mirror_group is not None and self._mirror_group.primary_dead()
        )
        # A part with a mirror copy cannot profit from a question to a
        # primary the breaker already took away: the answer is the
        # mirror's, and asking anyway is what held a download for the whole
        # reconnect cycle of the Telegram client -- far past the 20 s a
        # client waits before it drops an idle transfer -- while the mirror
        # that would have served the bytes waited to be asked. Parts with
        # no mirror copy are still asked for, because they are the only
        # copy, but on the short probe.
        skipped = (
            [
                mid
                for version in versions
                if version.owned_by(self.key) and self._fully_mirrored(version)
                for mid in version.message_ids
                if mid > 0
            ]
            if primary_down
            else []
        )
        asked_ids = [mid for mid in all_ids if mid not in set(skipped)]
        if skipped:
            logger.warning(
                f"Serving the parts of {fd.name} from the mirrors: the "
                f"primary store is marked down"
            )
            # Nothing was verified against the owner; the stat writer and
            # the backfill must not take this read as complete.
            fd.complete = False
        try:
            file_messages = (
                await _bounded_read(
                    f"the parts of {fd.name}",
                    self._message_api.get_messages(asked_ids),
                    timeout=(
                        UNREACHABLE_PROBE_TIMEOUT_S
                        if primary_down
                        else PART_LOOKUP_TIMEOUT_S
                    ),
                )
                if asked_ids
                else []
            )
        except Exception as ex:
            # An unreachable primary (banned channel, network down) must
            # not make every file look invalid while a mirror has it: treat
            # every part as missing here and let the mirror lookup below
            # decide.
            if not self._mirror_group:
                raise
            logger.warning(
                f"Could not check the primary store for the parts of {fd.name}: "
                f"{ex}; checking the mirrors"
            )
            if is_store_outage(ex):
                self._mirror_group.mark_primary_dead()
            file_messages = [None] * len(asked_ids)
            fd.complete = False

        message_map = {msg.message_id: msg for msg in file_messages if msg}
        acting_map, unavailable = await self._acting_map(versions)

        has_valid_version = False

        for i, version in enumerate(versions):
            owned = version.owned_by(self.key)
            acting = (
                not owned
                and self._mirror_group is not None
                and version.store is not None
                and version.store in self._mirror_group.store_keys
            )
            # Each version's ids are checked against the store that owns
            # them: the primary, or the acting store a failed-over write
            # went to. Anything else reads the mirrors.
            version.part_sizes = []
            primary_layout_ok = True
            for j, message_id in enumerate(version.message_ids):
                file_message = (
                    message_map.get(message_id)
                    if owned
                    else (
                        acting_map.get((version.store, message_id))
                        if acting and version.store is not None
                        else None
                    )
                )
                if file_message and file_message.document:
                    version.part_sizes.append(file_message.document.size)
                    continue

                # The primary copy of this part is gone -- before declaring
                # the version invalid, check whether a mirror still has it.
                mirror_candidates = [
                    (store_key, mirror_ids[j])
                    for store_key, mirror_ids in version.mirrors.items()
                    if j < len(mirror_ids) and mirror_ids[j] > 0
                ]
                mirror_sizes = await self._lookup_mirror_parts(mirror_candidates)
                if mirror_sizes:
                    if not acting or (version.store, message_id) not in mirror_sizes:
                        logger.warning(
                            f"File message {message_id} for part {j + 1} of "
                            f"{fd.name}@{version.id} not found in the owning "
                            f"store, serving from mirror"
                        )
                    version.part_sizes.append(next(iter(mirror_sizes.values())))
                    continue

                primary_layout_ok = False
                break

            if not primary_layout_ok:
                # Neither the primary nor an aligned mirror has every part.
                # A replica with its own layout still makes the version
                # readable; the primary layout is marked unusable by
                # leaving its part sizes empty.
                if await self._verify_replicas(version):
                    logger.warning(
                        f"{fd.name}@{version.id} has no complete aligned copy, "
                        f"serving from a replica"
                    )
                    version.part_sizes = []
                    version._size = INVALID_FILE_SIZE
                else:
                    if version.store in unavailable:
                        fd.complete = False
                    logger.warning(
                        f"File messages of {fd.name}@{version.id} not found in any store"
                    )
                    version.set_invalid()
            if version.is_valid():
                has_valid_version = True
                if not include_all_versions:
                    # Found a valid version, no need to check further
                    break

        return self._point_at_readable(fd, has_valid_version)

    def _point_at_readable(
        self, fd: TGFSFileDesc, has_valid_version: bool
    ) -> TGFSFileDesc:
        # A pending version is readable on the instance whose cache holds
        # it; elsewhere it counts as invalid, like a version whose parts
        # are gone. The version served is the latest one, so when that one
        # is unreadable the pointer moves to the newest readable version
        # (in memory only: the pointer is derived, never written). A
        # readable latest version is left alone, ties and all.
        def readable(version: TGFSFileVersion) -> bool:
            return version.is_valid() or self._readable_locally(version)

        if any(self._readable_locally(v) for v in fd.get_versions()):
            has_valid_version = True
        current = fd.versions.get(fd.latest_version_id)
        if current is None or not readable(current):
            candidates = [v for v in fd.versions.values() if readable(v)]
            if candidates:
                # ``max`` keeps the first of equally dated versions, as
                # ``from_dict`` does when it derives the pointer.
                fd.latest_version_id = max(
                    candidates, key=lambda v: v.updated_at_timestamp
                ).id

        if has_valid_version:
            return fd
        empty = TGFSFileDesc.empty(fd.name)
        empty.complete = fd.complete
        return empty

    async def _get_fd_text(self, fr: TGFSFileRef) -> Optional[str]:
        """Read the FD JSON, falling back to mirror copies if needed.

        ``None`` means every place that could hold the descriptor said it
        does not have it. A read that failed raises instead: a failed read
        is not a missing file, and reporting it as one is what turned a
        transient store problem into downloads of empty files.
        """
        message = None
        failure: Optional[Exception] = None
        acting = (
            self._mirror_group.store_for(fr.store)
            if self._mirror_group is not None
            and fr.store is not None
            and fr.store in self._mirror_group.store_keys
            else None
        )
        if acting is not None:
            # The acting store of a failed-over write owns the current
            # descriptor copy under ``message_id``; ``mirrors`` there may
            # hold a stale leftover from before the outage.
            try:
                message = (
                    await _bounded_read(
                        f"the read of file descriptor {fr.message_id} for {fr.name}",
                        acting.get_messages([fr.message_id]),
                    )
                )[0]
            except Exception as ex:
                failure = ex
                logger.warning(
                    f"Could not read file descriptor {fr.message_id} for "
                    f"{fr.name} from the acting store {fr.store}: {ex}"
                )
            if message and message.text:
                return message.text
            message = None
        # Skipping the primary is only worth it when a mirror copy of this
        # very descriptor exists: without one there is nothing else to read
        # from, and skipping turned one failed request into a minute of
        # "not found" -- and an empty file -- for every such file.
        skip_primary = (
            self._mirror_group is not None
            and self._mirror_group.primary_dead()
            and bool(fr.mirrors)
        )
        try:
            if acting is None and fr.owned_by(self.key) and not skip_primary:
                message = (
                    await _bounded_read(
                        f"the read of file descriptor {fr.message_id} for {fr.name}",
                        self._message_api.get_messages([fr.message_id]),
                        # A primary the breaker took away gets the short
                        # probe: this read runs before a download's first
                        # byte, and the client gives up on a silent
                        # transfer after 20 s. Nothing else holds this
                        # descriptor, so it is still worth asking -- just
                        # not for longer than the client waits.
                        timeout=(
                            UNREACHABLE_PROBE_TIMEOUT_S
                            if self._mirror_group is not None
                            and self._mirror_group.primary_dead()
                            else None
                        ),
                    )
                )[0]
        except Exception as ex:
            failure = ex
            logger.warning(
                f"Could not read file descriptor {fr.message_id} for "
                f"{fr.name} from the primary store: {ex}"
            )
            message = None
            # A request that could not be built is a problem with this
            # descriptor, not with the store: taking the primary away from
            # every other file for a minute is how one bad id became a
            # wave of empty downloads.
            if self._mirror_group and is_store_outage(ex):
                self._mirror_group.mark_primary_dead()

        if message and message.text:
            return message.text

        group = self._mirror_group
        for store_key, mirror_mid in fr.mirrors.items():
            if group is None or (api := group.store_for(store_key)) is None:
                continue
            try:
                mirror_message = (
                    await _bounded_read(
                        f"the read of file descriptor mirror {mirror_mid} in "
                        f"{store_key} for {fr.name}",
                        api.get_messages([mirror_mid]),
                    )
                )[0]
            except Exception as ex:
                logger.warning(
                    f"Could not read FD mirror {mirror_mid} in store "
                    f"{store_key} for {fr.name}: {ex}"
                )
                continue
            if mirror_message and mirror_message.text:
                logger.warning(
                    f"Serving file descriptor for {fr.name} from mirror "
                    f"store {store_key}"
                )
                return mirror_message.text
        if fr.store is not None and fr.store != self.key:
            # Nothing was even asked: the ref names a store this file system
            # does not have (a renamed or removed store). Say so instead of
            # leaving a bare "not found" and an empty file behind it.
            logger.warning(
                f"File descriptor {fr.message_id} of {fr.name} is recorded in "
                f"store {fr.store}, which is not configured here; it cannot be "
                f"read from {self.key}"
            )
        if failure is not None:
            raise TechnicalError(
                f"The file descriptor of {fr.name} could not be read: {failure}",
                cause=str(failure) or type(failure).__name__,
            )
        return None

    def _trust_recorded(self, fd: TGFSFileDesc) -> TGFSFileDesc:
        """Serve ``fd`` as the descriptor records it, without checking that
        the content messages still exist.

        For listings with ``listing.validate_on_list: false``: the sizes
        are the ones recorded when each version was written, and a version
        whose parts were deleted by hand only shows up as broken on read.
        """
        has_valid_version = False
        for version in fd.get_versions():
            if not version.is_valid():
                continue
            has_valid_version = True
            if version.size == INVALID_FILE_SIZE and version.recorded_size >= 0:
                version._size = version.recorded_size
        return self._point_at_readable(fd, has_valid_version)

    async def get(
        self,
        fr: TGFSFileRef,
        include_all_versions: bool = False,
        validate: bool = True,
    ) -> TGFSFileDesc:
        # After a promotion the ref may still describe the former primary;
        # re-express it relative to this store before touching anything.
        fr.relocate(self.key)
        self._rehome_ref(fr)
        try:
            text = await self._get_fd_text(fr)
        except TechnicalError as ex:
            # A read that failed is not a file that is gone: report it as
            # unreadable, so a listing still works and a download raises
            # instead of handing the client an empty file.
            logger.error(
                f"File descriptor (message_id: {fr.message_id}) for {fr.name} "
                f"could not be read: {ex}"
            )
            missing = TGFSFileDesc.empty(fr.name)
            missing.complete = False
            return missing

        if not text:
            logging.error(
                f"File descriptor (message_id: {fr.message_id}) for {fr.name} not found"
            )
            missing = TGFSFileDesc.empty(fr.name)
            missing.complete = False
            return missing

        fd = TGFSFileDesc.from_dict(json.loads(text), name=fr.name)
        for version in fd.get_versions():
            version.relocate(self.key)
        self._rehome_versions(fd)
        if not validate:
            return self._trust_recorded(fd)
        return await self._validate_fv(fd, include_all_versions)

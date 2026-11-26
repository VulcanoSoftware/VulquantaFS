"""Backfill: mirror pre-existing data into the mirror stores.

When redundancy is enabled on an existing installation, nothing that is
already in the primary store has a mirror copy. This module walks the metadata
tree (the authoritative work list -- everything VulquantaFS serves is reachable
from it), finds file versions without a complete mirror set, and copies
them using the same primitives as the live write path.

The job is idempotent and resumable for free: a version is skipped when
its ``mirrors`` map already covers every configured store, and the FD
edit that records the map is the commit point of each unit of work. A
crash between forward and commit merely leaves an orphaned copy in the
mirror store, which is harmless.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional, Tuple

from vulquantafs.core.local_cache import version_cache_for
from vulquantafs.core.mirror import MirrorStore
from vulquantafs.core.model import (
    TGFSDirectory,
    TGFSFileDesc,
    TGFSFileRef,
    TGFSFileVersion,
)
from vulquantafs.core.repository.interface import FDRepositoryResp
from vulquantafs.errors import TechnicalError
from vulquantafs.reqres import Replica, SentFileMessage
from vulquantafs.tasks import task_store
from vulquantafs.tasks.models import TaskStatus, TaskType

if TYPE_CHECKING:
    from vulquantafs.core.client import Client

logger = logging.getLogger(__name__)


def _storage_name(client: "Client", name: str) -> str:
    repo = getattr(client, "fc_repo", None)
    return repo.storage_name(name) if repo is not None else name


@dataclass
class BackfillReport:
    files_scanned: int = 0
    versions_checked: int = 0
    versions_mirrored: int = 0
    versions_promoted: int = 0
    # Write-back uploads moved from the local cache into the primary.
    versions_distributed: int = 0
    fds_mirrored: int = 0
    failures: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "files_scanned": self.files_scanned,
            "versions_checked": self.versions_checked,
            "versions_mirrored": self.versions_mirrored,
            "versions_promoted": self.versions_promoted,
            "fds_mirrored": self.fds_mirrored,
            "failures": self.failures,
        }


def collect_file_refs(directory: TGFSDirectory) -> List[TGFSFileRef]:
    refs = list(directory.find_files())
    for child in directory.find_dirs():
        refs.extend(collect_file_refs(child))
    return refs


async def _verify_mirrors(client: "Client", fd: TGFSFileDesc) -> None:
    """Drop mirror entries whose copies no longer exist.

    Mirror messages can be deleted manually just like primary ones; a
    dropped entry makes the version eligible for re-mirroring below.
    """
    if (mirror_group := client.mirror_group) is None:
        return
    for version in fd.get_versions(exclude_invalid=True):
        copies = [(key, ids) for key, ids in version.mirrors.items()] + [
            (key, replica.message_ids) for key, replica in version.replicas.items()
        ]
        for store_key, all_ids in copies:
            if store_key == version.store:
                continue
            if (api := mirror_group.store_for(store_key)) is None:
                continue
            ids = [mid for mid in all_ids if mid > 0]
            if not ids:
                continue
            try:
                messages = await api.get_messages(ids)
            except Exception as ex:
                logger.warning(
                    f"Verification of mirror store {store_key} failed "
                    f"for {fd.name}@{version.id}: {ex}"
                )
                continue
            if any(m is None or m.document is None for m in messages):
                logger.warning(
                    f"Mirror copy of {fd.name}@{version.id} in store "
                    f"{store_key} is incomplete, scheduling re-mirror"
                )
                version.mirrors.pop(store_key, None)
                version.replicas.pop(store_key, None)


async def _distribute_pending(
    client: "Client",
    fr: TGFSFileRef,
    fd: TGFSFileDesc,
    version: TGFSFileVersion,
    report: BackfillReport,
) -> bool:
    """Move a write-back upload from the local cache into the stores.

    The primary and every mirror that gets a replica are filled at the
    same time from the one local file; aligned mirrors follow through the
    usual path once the primary's ids exist. The primary's ids are
    committed as soon as they exist -- that is the moment the durability
    window of ``write_ack: cache`` closes. Returns whether the descriptor
    changed.
    """
    cache = getattr(client, "cache", None)
    local = cache.local(version.id) if cache is not None else None
    if local is None:
        # The window this mode admits: the bytes were only here, and here
        # they are gone. Say so loudly; the file falls back to its
        # previous version.
        message = (
            f"{fr.name}@{version.id}: the pending version is not in the local "
            f"cache any more; its {version.size} bytes are lost and the file "
            f"falls back to its previous version"
        )
        logger.error(message)
        try:
            task_id = await task_store.add_task(
                task_type=TaskType.DISTRIBUTION,
                path=f"/{client.name}{fr.location.absolute_path}/{fr.name}",
                filename=fr.name,
                size_total=version.size,
            )
            await task_store.update_task_progress(
                task_id, status=TaskStatus.FAILED, error_message=message
            )
        except Exception as ex:  # pragma: no cover - reporting must not fail the worker
            logger.warning(f"Could not record the lost version as a task: {ex}")
        fd.delete_version(version.id)
        return True

    primary = client.store
    group = client.mirror_group
    name = _storage_name(client, fr.name)
    planned = primary.plan_parts(local.size)
    replica_targets = (
        [ch for ch in group.stores if group.needs_replica(ch, planned)]
        if group is not None
        else []
    )
    replica_targets = [ch for ch in replica_targets if not version.has_copy_in(ch.key)]

    async def upload_primary() -> List[SentFileMessage]:
        from vulquantafs.core.local_cache import FileMessageFromCache

        return await primary.upload(FileMessageFromCache.new(local, name))

    async def replicate(ch: MirrorStore) -> tuple[str, Replica]:
        if group is None:  # pragma: no cover - replica_targets is empty then
            raise TechnicalError("no mirror group")
        return ch.key, await group.replicate_local(ch, local, name)

    primary_task = asyncio.create_task(upload_primary())
    replica_tasks = [asyncio.create_task(replicate(ch)) for ch in replica_targets]
    await asyncio.gather(primary_task, *replica_tasks, return_exceptions=True)

    changed = False
    for ch, task in zip(replica_targets, replica_tasks):
        if (error := task.exception()) is not None:
            report.failures.append(
                f"{fr.name}@{version.id}: could not replicate to {ch.key} ({error})"
            )
        else:
            key, replica = task.result()
            version.replicas[key] = replica
            changed = True
    if (error := primary_task.exception()) is not None:
        report.failures.append(
            f"{fr.name}@{version.id}: could not upload to the primary ({error})"
        )
        return changed
    sent = primary_task.result()
    version.materialize(
        primary.key, [m.message_id for m in sent], [m.size for m in sent]
    )
    report.versions_distributed += 1
    return True


async def backfill_file(
    client: "Client", fr: TGFSFileRef, verify: bool, report: BackfillReport
) -> None:
    """Bring one file's copies up to date: the unit of work of both the
    backfill task and the replication queue."""
    mirror_group = client.mirror_group
    fd_repo = client.fd_repo
    if fd_repo is None:
        return

    fd = await fd_repo.get(fr, include_all_versions=True)  # type: ignore[call-arg]
    if not fd.complete:
        # A store outage can make validation return a partial descriptor.
        # Do not drop queued work or save an incompletely checked version list.
        report.failures.append(f"{fr.name}: file descriptor is temporarily unavailable")
        return
    if not fd.get_versions():
        return

    # Write-back uploads first: a pending version has no store copy at all
    # yet. Its ids are committed on their own so a mirror failure later
    # cannot keep the primary's copy out of the metadata.
    pending = [v for v in fd.get_versions() if v.pending]
    if pending:
        distributed = False
        for version in pending:
            distributed |= await _distribute_pending(client, fr, fd, version, report)
        if distributed:
            resp = await fd_repo.save(fd, fr)
            await _sync_ref(client, fr, resp)
        if mirror_group is None:
            cache = getattr(client, "cache", None)
            if cache is not None:
                for version in pending:
                    if not version.pending:
                        cache.release(version.id)
            return

    if mirror_group is None:
        return
    if verify:
        await _verify_mirrors(client, fd)

    changed = False
    name = _storage_name(client, fr.name)
    for version in fd.get_versions(exclude_invalid=True):
        report.versions_checked += 1

        # A version that still lives in a former primary gets its copy in
        # the current primary first; that copy becomes its primary layout.
        if not version.owned_by(mirror_group.primary.key):
            try:
                logger.info(
                    "Restoring %s@%s from %s to primary store %s",
                    fr.name,
                    version.id,
                    version.store,
                    mirror_group.primary.key,
                )
                if await mirror_group.copy_into_primary(version, name=name):
                    report.versions_promoted += 1
                    changed = True
                else:
                    report.failures.append(
                        f"{fr.name}@{version.id}: could not copy into the primary"
                    )
            except Exception as ex:
                report.failures.append(
                    f"{fr.name}@{version.id}: could not copy into the primary ({ex})"
                )

        cache = version_cache_for(
            getattr(client, "cache", None), client.name, version.id, version.size
        )
        missing = mirror_group.missing_stores(version)
        if not missing:
            if cache is not None:
                cache.release()
            continue
        if not version.owned_by(mirror_group.primary.key):
            # No primary copy to mirror from yet; the next run retries.
            continue
        copies = await mirror_group.mirror_parts(
            version.message_ids,
            version.part_sizes or None,
            only_stores=missing,
            cache=cache,
            name=name,
        )
        if copies.store_keys:
            copies.apply_to(version)
            report.versions_mirrored += 1
            changed = True
        still_missing = [key for key in missing if key not in copies.store_keys]
        if still_missing:
            report.failures.append(
                f"{fr.name}@{version.id}: could not mirror to "
                f"{', '.join(still_missing)}"
            )
        elif cache is not None:
            # Every mirror has its copy: the staged bytes are no longer
            # needed for replication.
            cache.release()

    fd_missing = (
        set(mirror_group.store_keys)
        - {key for key, mid in fr.mirrors.items() if mid > 0}
        - {fr.store}
    )

    if changed or fd_missing:
        # Commit point: the (possibly updated) mirrors map is persisted
        # in the FD message, and the FD itself gets its mirror copies.
        resp = await fd_repo.save(fd, fr)
        await _sync_ref(client, fr, resp)
        descriptors_missing = (
            fd_missing
            - {key for key, mid in fr.mirrors.items() if mid > 0}
            - {fr.store}
        )
        if descriptors_missing:
            report.failures.append(
                f"{fr.name}: could not refresh the descriptor in "
                f"{', '.join(sorted(descriptors_missing))}"
            )
        if fd_missing - descriptors_missing:
            report.fds_mirrored += 1


async def _sync_ref(
    client: "Client", fr: TGFSFileRef, resp: "FDRepositoryResp"
) -> None:
    if (
        resp.mirrors != fr.mirrors
        or resp.message_id != fr.message_id
        or resp.store != fr.store
    ):
        if client.metadata_api and (
            fr.message_id != resp.message_id or fr.store != resp.store
        ):
            await client.metadata_api.flush_uploads(ref=fr)
        fr.message_id = resp.message_id
        fr.mirrors = dict(resp.mirrors)
        fr.store = resp.store


async def backfill_mirrors(
    client: "Client",
    verify: bool = False,
    task_id: Optional[str] = None,
) -> BackfillReport:
    """Mirror every unmirrored file version of ``client``'s file system.

    Files are processed newest first so the most recent data is
    protected earliest. Failures are recorded and skipped -- rerunning
    the job retries exactly the missing pieces.
    """
    report = BackfillReport()

    if client.mirror_group is None or client.fd_repo is None:
        report.failures.append(
            f"File system '{client.name}' has no mirror stores configured"
        )
        return report

    refs = collect_file_refs(client.dir_api.root)

    # Newest first: load descriptors to know each file's timestamp.
    dated: List[Tuple[TGFSFileRef, int]] = []
    for fr in refs:
        try:
            fd = await client.fd_repo.get(fr)
            dated.append((fr, fd.updated_at_timestamp))
        except Exception as ex:
            report.failures.append(f"{fr.name}: cannot read descriptor ({ex})")
    dated.sort(key=lambda pair: pair[1], reverse=True)

    if task_id:
        await task_store.update_task_progress(task_id, status=TaskStatus.IN_PROGRESS)

    metadata_dirty = False
    for fr, _ in dated:
        before = (fr.message_id, dict(fr.mirrors), fr.store)
        try:
            await backfill_file(client, fr, verify, report)
        except Exception as ex:
            report.failures.append(f"{fr.name}: {ex}")
            logger.error(f"Backfill failed for {fr.name}: {ex}")
        if before != (fr.message_id, fr.mirrors, fr.store):
            metadata_dirty = True
        report.files_scanned += 1
        if task_id:
            await task_store.update_task_progress(task_id, size_delta=1)

    if metadata_dirty and client.metadata_api:
        await client.metadata_api.push()

    if task_id:
        await task_store.update_task_progress(
            task_id,
            status=(TaskStatus.COMPLETED if not report.failures else TaskStatus.FAILED),
            error_message="; ".join(report.failures) or None,
        )

    logger.info(
        f"Backfill for '{client.name}' finished: "
        f"{report.versions_mirrored}/{report.versions_checked} versions "
        f"mirrored, {len(report.failures)} failures"
    )
    return report


async def create_backfill_task(client: "Client", total_files: int) -> str:
    return await task_store.add_task(
        task_type=TaskType.MIRROR_BACKFILL,
        path=f"/{client.name}",
        filename=f"mirror-backfill-{client.name}",
        size_total=total_files,
    )


def count_files(client: "Client") -> int:
    return len(collect_file_refs(client.dir_api.root))

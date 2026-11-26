import asyncio
import datetime
import logging
from typing import (
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)
from uuid import uuid4 as uuid

from vulquantafs.backends.base import IStore
from vulquantafs.core.mirror import MirrorGroup
from vulquantafs.core.model import (
    TGFSDirectory,
    TGFSFileDesc,
    TGFSFileRef,
    TGFSFileVersion,
)
from vulquantafs.core.repository.interface import FDRepositoryResp
from vulquantafs.errors import (
    FileOrDirectoryDoesNotExist,
    PendingCommitError,
    TechnicalError,
)
from vulquantafs.reqres import (
    FileContent,
    FileMessage,
    FileMessageEmpty,
)

from .file_desc import FileDescApi
from .metadata import MetaDataApi
from .stat import FileStatApi

logger = logging.getLogger(__name__)

# {mirror store key -> message ids to delete there}
MirrorMessageIds = Dict[str, List[int]]


def _merge_mirror_ids(target: MirrorMessageIds, source: Dict[str, int]) -> None:
    for channel_key, mid in source.items():
        if mid > 0:
            target.setdefault(channel_key, []).append(mid)


class FileApi:
    def __init__(
        self,
        metadata_api: MetaDataApi,
        file_desc_api: FileDescApi,
        message_api: IStore,
        mirror_group: Optional[MirrorGroup] = None,
        inline_mirroring: bool = True,
        on_written: Optional[Callable[[TGFSFileRef], Awaitable[None]]] = None,
        written_gaps_only: bool = False,
        stat_api: Optional[FileStatApi] = None,
        cleanup_local_versions: bool = True,
        has_cached_versions: Optional[Callable[[], bool]] = None,
    ):
        self._metadata_api = metadata_api
        self._file_desc_api = file_desc_api
        self._message_api = message_api
        self._mirror_group = mirror_group
        self._inline_mirroring = inline_mirroring
        self._cleanup_local_versions = cleanup_local_versions
        self._has_cached_versions = has_cached_versions
        # Called after a file's content changed and the metadata is
        # pushed; the replication queue hooks in here for
        # ``sync: background``.
        self._on_written = on_written
        # ``sync: inline`` or ``sync: tee``: the write itself filled the
        # mirrors (re-uploading ones from the stream, the others copied
        # server-side), so the queue is only told about a file some store
        # still lacks a copy of (a mirror that failed mid-stream, a
        # server-side copy that was refused, a copy of a file made without
        # a stream). In background mode every write is queued.
        self._written_gaps_only = written_gaps_only
        # What listings show of a file; see ``FileStatApi``.
        self.stats = stat_api or FileStatApi(file_desc_api)

    async def _written(self, fr: TGFSFileRef, fd: Optional[TGFSFileDesc]) -> None:
        if self._on_written is None:
            return
        if self._written_gaps_only and fd is not None and self._mirror_group:
            # A write that filled the mirrors itself only hands its gaps to
            # the queue: a store that lacks a copy, and a version that a
            # failed-over acting store owns while the primary has none.
            if not any(
                self._mirror_group.missing_stores(version)
                or not version.owned_by(self._store_key)
                for version in fd.get_versions()
                if version.is_valid()
            ):
                return
        await self._on_written(fr)

    async def collect_message_ids(self, fr: TGFSFileRef) -> List[int]:
        """Return every primary-store message id backing ``fr``.

        Includes the file descriptor message itself plus every content
        message across all known versions. Returns just the descriptor
        id if the descriptor can no longer be read (it may already be
        gone), so callers can still try to delete that one message.
        """
        primary, _ = await self.collect_all_message_ids(fr)
        return primary

    async def collect_all_message_ids(
        self, fr: TGFSFileRef, version_ids: Optional[List[str]] = None
    ) -> Tuple[List[int], MirrorMessageIds]:
        """Like :meth:`collect_message_ids`, but also returns the message
        ids of every mirrored copy, grouped by mirror store. With
        ``version_ids`` given, the ids of the versions seen are appended
        to it (for dropping their local cache entries)."""
        ids: List[int] = []
        mirror_ids: MirrorMessageIds = {}
        try:
            # The recorded ids are enough for best-effort deletion; don't
            # ask Telegram/Discord whether every content part still exists.
            fd = await self._file_desc_api.get_file_desc(fr, validate=False)
        except Exception as ex:
            logger.warning(
                f"Could not load file descriptor for {fr.name} "
                f"(message_id={fr.message_id}): {ex}"
            )
            fd = None
        # After get_file_desc the ref is relocated, so its ids are sorted
        # into the right store below.
        self._collect_ref_ids(fr, ids, mirror_ids)
        if fd is None:
            return ids, mirror_ids
        for version in fd.get_versions():
            self._collect_version_ids(version, ids, mirror_ids)
            if version_ids is not None:
                version_ids.append(version.id)
        return ids, mirror_ids

    @property
    def _store_key(self) -> str:
        return self._message_api.key

    def _collect_ref_ids(
        self, fr: TGFSFileRef, ids: List[int], mirror_ids: MirrorMessageIds
    ) -> None:
        """Sort the descriptor id into the primary or the store it belongs to.

        An id that belongs to a former primary must never be deleted
        from the current one: it names an unrelated message there.
        """
        if fr.message_id > 0:
            if fr.owned_by(self._store_key):
                ids.append(fr.message_id)
            elif fr.store:
                mirror_ids.setdefault(fr.store, []).append(fr.message_id)
        _merge_mirror_ids(mirror_ids, fr.mirrors)

    def _collect_version_ids(
        self, version: TGFSFileVersion, ids: List[int], mirror_ids: MirrorMessageIds
    ) -> None:
        owned = [mid for mid in version.message_ids if mid > 0]
        if version.owned_by(self._store_key):
            ids.extend(owned)
        elif version.store:
            mirror_ids.setdefault(version.store, []).extend(owned)
        for channel_key, version_mirror_ids in version.mirrors.items():
            mirror_ids.setdefault(channel_key, []).extend(
                mid for mid in version_mirror_ids if mid > 0
            )
        for channel_key, replica in version.replicas.items():
            target = (
                ids
                if channel_key == self._store_key
                else (mirror_ids.setdefault(channel_key, []))
            )
            target.extend(mid for mid in replica.message_ids if mid > 0)

    async def _collect_version_message_ids(
        self, fr: TGFSFileRef, version_id: str
    ) -> Tuple[List[int], MirrorMessageIds]:
        try:
            fd = await self._file_desc_api.get_file_desc(fr, validate=False)
            version = fd.get_version(version_id)
        except Exception as ex:
            logger.warning(f"Could not load version {version_id} of {fr.name}: {ex}")
            return [], {}
        ids: List[int] = []
        mirror_ids: MirrorMessageIds = {}
        self._collect_version_ids(version, ids, mirror_ids)
        return ids, mirror_ids

    async def delete_mirrored(
        self, mirror_ids: MirrorMessageIds, force: bool = False
    ) -> None:
        if self._mirror_group and mirror_ids:
            await self._mirror_group.delete(mirror_ids, force=force)

    @classmethod
    def _iter_file_refs(cls, directory: TGFSDirectory) -> Iterator[TGFSFileRef]:
        yield from directory.find_files()
        for child in directory.find_dirs():
            yield from cls._iter_file_refs(child)

    def _descriptors_referenced_elsewhere(
        self, removing: Iterable[TGFSFileRef]
    ) -> Set[int]:
        """Descriptor message ids that survive removing ``removing``.

        Copies used to be made by pointing a second file ref at the *same*
        descriptor message, so both files were backed by one set of Telegram
        messages. Removing either of them would then leave the other one
        pointing at messages that no longer exist -- reported by WebDAV as a
        0-byte file. New copies own their content, but metadata written
        before that still holds such pairs, so removals keep checking.
        """
        removing_ids = {id(fr) for fr in removing}
        return {
            fr.message_id
            for fr in self._iter_file_refs(self._metadata_api.get_root_directory())
            if id(fr) not in removing_ids and fr.message_id > 0
        }

    async def collect_deletable_message_ids(
        self, frs: Sequence[TGFSFileRef], version_ids: Optional[List[str]] = None
    ) -> Tuple[List[int], MirrorMessageIds]:
        """Message ids that become garbage once ``frs`` are removed.

        Refs whose descriptor is still referenced by a file outside ``frs``
        contribute nothing: their messages are shared and must stay.
        ``version_ids`` collects the ids of the versions that go with them.
        """
        if not self._deletes_backend_messages() and not (
            version_ids is not None and self._cleanup_local_versions
        ):
            # A metadata-only delete needs neither a scan for shared
            # descriptors nor any descriptor reads from the backends.
            return [], {}
        shared = self._descriptors_referenced_elsewhere(frs)
        ids: List[int] = []
        mirror_ids: MirrorMessageIds = {}
        for fr in frs:
            if fr.message_id in shared:
                logger.info(
                    f"Keeping the telegram messages of {fr.name} "
                    f"(descriptor {fr.message_id}): another file still refers to them"
                )
                continue
            fr_ids, fr_mirror_ids = await self.collect_all_message_ids(fr, version_ids)
            ids.extend(fr_ids)
            for channel_key, channel_ids in fr_mirror_ids.items():
                mirror_ids.setdefault(channel_key, []).extend(channel_ids)
        return ids, mirror_ids

    def _deletes_backend_messages(self) -> bool:
        return self._message_api.deletes_on_remove or (
            self._mirror_group is not None and self._mirror_group.deletes_on_remove
        )

    def _owning_store(self, version: TGFSFileVersion) -> Optional[IStore]:
        """The store whose channel ``version.message_ids`` live in.

        A version without a recorded store belongs to the primary, as
        tgfs always assumed. Versions of a failed-over write live in the
        acting store that took them. ``None`` when that store is not
        configured any more; a copy would duplicate the wrong message.
        """
        if version.owned_by(self._store_key):
            return self._message_api
        if self._mirror_group is not None and version.store is not None:
            return self._mirror_group.store_for(version.store)
        return None

    async def _duplicate_versions(self, fd: TGFSFileDesc) -> TGFSFileDesc:
        """Build a descriptor whose versions own fresh content messages.

        Every part of every version is duplicated inside the channel in one
        server-side call, so the copy costs the same whether the file is a
        kilobyte or a hundred gigabytes. Timestamps and part sizes carry
        over, so the copy keeps the full history; the versions get fresh ids
        because two files that are meant to be independent should not share
        an identifier with each other.
        """
        versions = fd.get_versions(sort=True)
        # A copy is a channel-internal duplicate of content that lives in
        # the store that owns it: the primary, or -- after a write
        # failover, possibly with the primary never seen again -- an
        # acting store. Duplicating in the wrong channel would either
        # fail or duplicate unrelated messages.
        copied_parts: Dict[str, List[int]] = {}
        for version in versions:
            if not version.message_ids:
                continue
            target = self._owning_store(version)
            if target is None:
                raise TechnicalError(
                    f"Cannot copy {fd.name}@{version.id}: its store "
                    f"{version.store} is not configured"
                )
            copied_parts[version.id] = await target.copy_within(version.message_ids)

        copied = TGFSFileDesc(name=fd.name)
        version_ids: Dict[str, str] = {}
        for version in versions:
            version_ids[version.id] = str(uuid())
            new_version = TGFSFileVersion(
                id=version_ids[version.id],
                updated_at=version.updated_at,
                message_ids=copied_parts.get(version.id, []),
                part_sizes=list(version.part_sizes),
                store=version.store or self._store_key,
            )
            # Copies are mirrored version by version: a replica covers one
            # version's bytes, not the whole file's.
            if self._mirror_group and self._inline_mirroring and new_version.is_valid():
                copies = await self._mirror_group.mirror_parts(
                    new_version.message_ids,
                    new_version.part_sizes or None,
                    source=self._owning_store(new_version),
                    name=fd.name,
                )
                copies.apply_to(new_version)
            copied.add_version(new_version)
        # add_version derives created_at from the versions it is given; the
        # copy should carry the original's creation date instead.
        copied.created_at = fd.created_at
        if latest := version_ids.get(fd.latest_version_id):
            copied.latest_version_id = latest
        return copied

    async def copy(
        self, where: TGFSDirectory, fr: TGFSFileRef, name: Optional[str] = None
    ) -> TGFSFileRef:
        """Copy a file without moving a single byte of its content.

        The copy gets its own descriptor and its own content messages, so
        writing to it -- or deleting it -- leaves the original alone.
        """
        fd = await self._file_desc_api.get_file_desc(fr, include_all_versions=True)
        copied_fd = await self._duplicate_versions(fd)
        resp: Optional[FDRepositoryResp] = None
        try:
            resp = await self._file_desc_api.save_new_file_desc(copied_fd)
            copied_fr = await where.create_file_ref_async(
                name or fr.name, resp.message_id
            )
        except Exception:
            await self._discard_orphans(copied_fd, resp)
            raise
        copied_fr.mirrors = dict(resp.mirrors)
        copied_fr.store = resp.store
        await self._metadata_api.push()
        await self._written(copied_fr, copied_fd)
        return copied_fr

    async def _discard_orphans(
        self, fd: TGFSFileDesc, resp: Optional[FDRepositoryResp] = None
    ) -> None:
        """Drop the messages of a copy that never made it into the metadata.

        Nothing refers to them, and they were created moments ago by this
        very call, so they are cleaned up regardless of the user's setting
        for keeping removed file messages.
        """
        message_ids: List[int] = []
        mirror_ids: MirrorMessageIds = {}
        if resp is not None:
            message_ids.append(resp.message_id)
            _merge_mirror_ids(mirror_ids, resp.mirrors)
        for version in fd.get_versions():
            self._collect_version_ids(version, message_ids, mirror_ids)
        try:
            await self._message_api.delete_messages(message_ids, force=True)
            await self.delete_mirrored(mirror_ids, force=True)
        except Exception as ex:
            logger.warning(f"Could not clean up after a failed copy: {ex}")

    async def move(
        self, fr: TGFSFileRef, where: TGFSDirectory, name: Optional[str] = None
    ) -> TGFSFileRef:
        """Relocate ``fr`` without re-uploading or deleting anything.

        Implementing a move as copy-then-remove would delete the very
        messages the copy points at, so the moved file has to be relocated in
        the metadata instead.
        """
        from_dir = fr.location
        await self._metadata_api.flush_uploads(ref=fr)
        moved_fr = await from_dir.relocate_file_ref_async(fr, where, name)
        await self._metadata_api.push()
        self.stats.changed(from_dir, pushed=True)
        self.stats.changed(where, pushed=True)
        return moved_fr

    async def _create_new_file(
        self,
        where: TGFSDirectory,
        file_msg: FileMessage,
        defer_file_ref: bool = False,
        journal_file_ref: bool = False,
    ) -> TGFSFileDesc:
        resp = await self._file_desc_api.create_file_desc(file_msg)
        if journal_file_ref:
            # Journal every stored upload, whichever store took the bytes.
            # While the primary is down the acting store holds the content
            # and the reference belongs to that store, but waiting for its
            # commit on GitHub is what turned a bulk upload into failed
            # transfers: a stats commit can hold the branch past the
            # deadline, and the client then retried into a name that was
            # already there. The journal makes the reference durable here
            # first and commits it in the background.
            staged = self._metadata_api.stage_upload(
                where, file_msg.name, resp.message_id, resp.mirrors, resp.store
            )
            if staged is not None:
                await self.stats.record(staged, resp.fd)
                await self._written(staged, resp.fd)
                return resp.fd
        if defer_file_ref:
            fr = await where.create_file_ref_deferred(file_msg.name, resp.message_id)
            pending = True
        else:
            fr, pending = await self._await_file_ref(
                where, file_msg.name, resp.message_id
            )
        fr.mirrors = dict(resp.mirrors)
        fr.store = resp.store
        if defer_file_ref or pending:
            # Legacy non-blocking create, or a reference whose commit is
            # still running: the name is in the tree and its content is
            # stored, so only the mirror sync is left to schedule.
            self._metadata_api.request_push()
        elif where.file_refs_persist_on_create and fr.owned_by(self._store_key):
            # The pathname and its primary descriptor are already durable
            # in the git tree; a mirror re-sync must not hold up CLOSE.
            self._metadata_api.request_push()
        else:
            # The descriptor already exists. Even if CLOSE loses its
            # connection, let the metadata write resolve: deleting the
            # ref during the write could hide a file which actually landed.
            commit = asyncio.create_task(self._push_new_file_ref(fr))
            try:
                await asyncio.shield(commit)
            except asyncio.CancelledError:
                await asyncio.gather(commit, return_exceptions=True)
                raise
        await self.stats.record(fr, resp.fd)
        await self._written(fr, resp.fd)
        return resp.fd

    async def _await_file_ref(
        self, where: TGFSDirectory, name: str, message_id: int
    ) -> tuple[TGFSFileRef, bool]:
        """Create the reference and wait for its commit; report a commit
        that is still running as a completed write.

        Past its deadline the commit worker keeps retrying while the
        reference is already in the tree, so the file is listed and
        readable straight away. Answering an error for it made the client
        retry the upload, find the very name it had just written and ask
        its user whether to overwrite -- once for every file of a bulk run
        that missed the bound. Only a create that left no reference behind
        is a failure.
        """
        try:
            return await where.create_file_ref_async(name, message_id), False
        except PendingCommitError as ex:
            found = [
                ref for ref in where.find_files([name]) if ref.message_id == message_id
            ]
            if not found:
                raise
            logger.warning(
                f"{name} is in the tree with message_id={message_id} and its "
                f"content is stored; only the metadata commit is still "
                f"running: {ex}"
            )
            return found[0], True

    async def _push_new_file_ref(self, fr: TGFSFileRef) -> None:
        try:
            await self._metadata_api.push(require_success=True)
        except Exception:
            where = fr.location
            if not where.file_refs_persist_on_create and any(
                current is fr for current in where.find_files([fr.name])
            ):
                where.unlink_file_ref(fr)
            raise

    async def _sync_file_ref(self, fr: TGFSFileRef, resp: FDRepositoryResp) -> None:
        """
        Sync the file ref with the descriptor's current location: the primary
        message_id may change if the original message went missing (e.g. it was
        manually deleted), and the mirror map changes whenever mirror copies
        are (re)written.
        """
        if (
            fr.message_id != resp.message_id
            or fr.mirrors != resp.mirrors
            or fr.store != resp.store
        ):
            if fr.message_id != resp.message_id or resp.store not in (
                None,
                self._store_key,
            ):
                await self._metadata_api.flush_uploads(ref=fr)
            fr.message_id = resp.message_id
            fr.mirrors = dict(resp.mirrors)
            fr.store = resp.store
            await self._metadata_api.push()

    async def _update_existing_file(
        self, fr: TGFSFileRef, file_msg: FileMessage, version_id: Optional[str]
    ) -> TGFSFileDesc:
        try:
            if version_id:
                resp = await self._file_desc_api.update_file_version(
                    fr, file_msg, version_id
                )
            else:
                resp = await self._file_desc_api.append_file_version(file_msg, fr)
            await self._sync_file_ref(fr, resp)
        except BaseException:
            # Also when the write failed halfway: the descriptor may have
            # changed already.
            self.stats.invalidate(fr)
            raise
        await self.stats.refresh(fr, resp.fd)
        await self._written(fr, resp.fd)
        return resp.fd

    async def set_last_modified(
        self, fr: TGFSFileRef, when: datetime.datetime
    ) -> TGFSFileDesc:
        """Give ``fr``'s latest version the modification time ``when``.

        WebDAV PROPPATCH after an upload; the descriptor message changes,
        the metadata only when the descriptor moved.
        """
        try:
            resp = await self._file_desc_api.set_last_modified(fr, when)
            await self._sync_file_ref(fr, resp)
        except BaseException:
            self.stats.invalidate(fr)
            raise
        await self.stats.refresh(fr, resp.fd)
        return resp.fd

    async def rm(
        self,
        fr: TGFSFileRef,
        version_id: Optional[str] = None,
        defer_metadata: bool = False,
    ) -> None:
        if not version_id:
            version_ids: List[str] = []
            clean_cache = self._cleanup_local_versions and (
                self._has_cached_versions is None or self._has_cached_versions()
            )
            message_ids, mirror_ids = await self.collect_deletable_message_ids(
                [fr], version_ids if clean_cache else None
            )
            await self._metadata_api.flush_uploads(ref=fr)
            if (
                defer_metadata
                and not self._deletes_backend_messages()
                and fr.owned_by(self._store_key)
                and self._metadata_api.defer_removal(fr)
            ):
                self.stats.changed(fr.location, pushed=True)
                if version_ids:
                    await self._file_desc_api.forget_versions(version_ids)
                return
            await fr.delete_async()
            await self._metadata_api.push()
            self.stats.changed(fr.location, pushed=True)
            await self._message_api.delete_messages(message_ids)
            await self.delete_mirrored(mirror_ids)
            if version_ids:
                await self._file_desc_api.forget_versions(version_ids)
        else:
            message_ids, mirror_ids = await self._collect_version_message_ids(
                fr, version_id
            )
            try:
                resp = await self._file_desc_api.delete_file_version(fr, version_id)
                await self._sync_file_ref(fr, resp)
            finally:
                self.stats.invalidate(fr)
            await self._message_api.delete_messages(message_ids)
            await self.delete_mirrored(mirror_ids)

    async def upload(
        self,
        under: TGFSDirectory,
        file_msg: FileMessage,
        version_id: Optional[str] = None,
        defer_file_ref: bool = False,
        journal_file_ref: bool = False,
    ) -> TGFSFileDesc:
        try:
            fr = under.find_file(file_msg.name)
            return await self._update_existing_file(fr, file_msg, version_id)
        except FileOrDirectoryDoesNotExist:
            return await self._create_new_file(
                under, file_msg, defer_file_ref, journal_file_ref
            )

    async def desc(
        self, fr: TGFSFileRef, include_all_versions: bool = False
    ) -> TGFSFileDesc:
        return await self._file_desc_api.get_file_desc(fr, include_all_versions)

    async def retrieve(
        self,
        fr: TGFSFileRef,
        begin: int,
        end: int,
        as_name: str,
    ) -> FileContent:
        fd = await self.desc(fr)
        if isinstance(fd, FileMessageEmpty):

            async def empty_file() -> FileContent:
                yield b""

            return empty_file()
        fv = fd.get_latest_version()
        if not fv.message_ids and not fv.pending:
            # No readable version: either the descriptor itself could not be
            # read, or every content message it lists is gone. Serving that
            # as a zero-byte file made a broken read look like a successful
            # download of an empty file, while the size in the listing kept
            # showing the real one. An error lets the client retry.
            recorded = fr.stat.size if fr.stat is not None else 0
            if not fd.complete or recorded > 0:
                raise TechnicalError(
                    f"{fr.name} has no readable version: "
                    + (
                        "its file descriptor could not be read"
                        if not fd.complete
                        else f"none of its {recorded} bytes are still stored"
                    )
                )

        async def chunks():
            try:
                async for chunk in await self._file_desc_api.download_file_at_version(
                    fv, begin, end, as_name or fr.name
                ):
                    yield chunk

            except Exception as ex:
                raise ex

        return chunks()

    async def retrieve_version(
        self,
        fv: TGFSFileVersion,
        begin: int,
        end: int,
        as_name: str,
    ) -> FileContent:
        return await self._file_desc_api.download_file_at_version(
            fv,
            begin,
            end,
            as_name,
        )

import json
import logging
from itertools import chain
from typing import Callable, Dict, List, Optional, Tuple

from tgdcfs.backends.base import IStore
from tgdcfs.core.mirror import MirrorGroup
from tgdcfs.core.model import TGFSFileDesc, TGFSFileRef, TGFSFileVersion
from tgdcfs.core.model.file import INVALID_FILE_SIZE
from tgdcfs.core.repository.interface import (
    FDRepositoryResp,
    IFDRepository,
)
from tgdcfs.errors import MessageNotFound, TechnicalError
from tgdcfs.reqres import MessageResp

logger = logging.getLogger(__name__)


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
                messages = await api.get_messages(mids)
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
                messages = await store.get_messages(replica.message_ids)
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
    ) -> Dict[int, MessageResp]:
        """Look up the parts of acting-owned versions in their stores.

        A version a failed-over write owns lives in a live mirror; its
        ids are checked there instead of against a primary that never
        saw the parts (and is likely down). A store that cannot answer
        simply contributes nothing, like a mirror below.
        """
        if self._mirror_group is None:
            return {}
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
        res: Dict[int, MessageResp] = {}
        for store_key, ids in by_store.items():
            api = self._mirror_group.store_for(store_key)
            if api is None:
                continue
            try:
                found = await api.get_messages(ids)
            except Exception as ex:
                logger.warning(
                    f"Could not check the acting store {store_key} for the "
                    f"parts it owns: {ex}"
                )
                continue
            res.update({m.message_id: m for m in found if m})
        return res

    async def _validate_fv(
        self, fd: TGFSFileDesc, include_all_versions: bool
    ) -> TGFSFileDesc:
        versions = fd.get_versions(exclude_invalid=True)

        # Files in the channel may be deleted manually, so we need to check if the messages for the versions exist.
        # Only ids that belong to this store are looked up here; a version
        # relocated from a former primary is checked against the mirrors.

        all_ids = list(
            chain(
                *(
                    version.message_ids
                    for version in versions
                    if version.owned_by(self.key)
                )
            )
        )
        try:
            file_messages = await self._message_api.get_messages(all_ids)
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
            self._mirror_group.mark_primary_dead()
            file_messages = [None] * len(all_ids)
            fd.complete = False

        message_map = {msg.message_id: msg for msg in file_messages if msg}
        acting_map = await self._acting_map(versions)

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
            own_map = message_map if owned else acting_map if acting else None
            version.part_sizes = []
            primary_layout_ok = True
            for j, message_id in enumerate(version.message_ids):
                if (
                    own_map is not None
                    and (file_message := own_map.get(message_id, None))
                    and file_message.document
                ):
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
                    logger.warning(
                        f"File message {message_id} for part {j + 1} of "
                        f"{fd.name}@{version.id} not found in the primary "
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
        """Read the FD JSON, falling back to mirror copies if needed."""
        message = None
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
                message = (await acting.get_messages([fr.message_id]))[0]
            except Exception as ex:
                logger.warning(
                    f"Could not read file descriptor {fr.message_id} for "
                    f"{fr.name} from the acting store {fr.store}: {ex}"
                )
            if message and message.text:
                return message.text
            message = None
        try:
            if acting is None and fr.owned_by(self.key):
                message = (await self._message_api.get_messages([fr.message_id]))[0]
        except Exception as ex:
            logger.warning(
                f"Could not read file descriptor {fr.message_id} for "
                f"{fr.name} from the primary store: {ex}"
            )
            message = None
            if self._mirror_group:
                self._mirror_group.mark_primary_dead()

        if message and message.text:
            return message.text

        if not self._mirror_group:
            return None

        for store_key, mirror_mid in fr.mirrors.items():
            if (api := self._mirror_group.store_for(store_key)) is None:
                continue
            try:
                mirror_message = (await api.get_messages([mirror_mid]))[0]
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
        text = await self._get_fd_text(fr)

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
        if not validate:
            return self._trust_recorded(fd)
        return await self._validate_fv(fd, include_all_versions)

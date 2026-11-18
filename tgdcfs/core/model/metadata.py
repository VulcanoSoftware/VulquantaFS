from dataclasses import dataclass
from typing import Optional

from .directory import TGFSDirectory


@dataclass
class TGFSMetadata:
    dir: TGFSDirectory
    # Time of the metadata's most recent write, as a Unix epoch in seconds.
    # The replicated metadata layer stamps it before every push so the
    # stores can be compared for freshness at load; git tree stores report
    # the branch head commit's date instead. A document written before
    # revisions existed reads as None and ranks below any dated copy.
    revision: Optional[float] = None

    @staticmethod
    def from_dict(data: dict) -> "TGFSMetadata":
        return TGFSMetadata(
            dir=TGFSDirectory.from_dict(data["dir"]),
            revision=data.get("revision"),
        )

    def to_dict(self) -> dict:
        document: dict = {
            "type": "TGFSMetadata",
            "dir": self.dir.to_dict(),
        }
        if self.revision is not None:
            document["revision"] = self.revision
        return document

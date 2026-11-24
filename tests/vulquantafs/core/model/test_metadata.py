"""The metadata document envelope: the freshness revision and its
backward compatibility."""

from vulquantafs.core.model import TGFSDirectory, TGFSMetadata


def metadata() -> TGFSMetadata:
    return TGFSMetadata(dir=TGFSDirectory.root_dir())


def test_the_revision_roundtrips():
    document = metadata()
    document.revision = 1760000000.5

    restored = TGFSMetadata.from_dict(document.to_dict())

    assert restored.revision == 1760000000.5


def test_a_document_without_a_revision_reads_as_unknown():
    assert TGFSMetadata.from_dict(metadata().to_dict()).revision is None

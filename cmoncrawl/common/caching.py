import os
import logging
import hashlib
from pathlib import Path
from cmoncrawl.common.types import DomainRecord

logger = logging.getLogger(__name__)


def cache_key(record: DomainRecord):
    """Returns an opaque key / filename for caching a `DomainRecord`."""
    h = hashlib.sha256()
    h.update(record.filename.encode())
    h.update("|".encode())
    h.update(str(record.offset).encode())
    h.update("|".encode())
    h.update(str(record.length).encode())
    return f"{h.hexdigest()}.bin"


class AbstractDomainRecordCache:
    """Cache interface for DomainRecords."""

    def get(self, record: DomainRecord) -> bytes | None:
        raise NotImplementedError

    def set(self, record: DomainRecord, data: bytes) -> None:
        raise NotImplementedError


class DomainRecordFilesystemCache(AbstractDomainRecordCache):
    """A local filesystem cache.

    If `cache_dir` does not exist, the implementation will attempt
    to create it upon first `set()` using `os.makedirs`.

    Entries are never pruned (no TTL support currently).
    """

    def __init__(self, cache_dir: Path):
        super().__init__()
        self.cache_dir = cache_dir

    def get(self, record: DomainRecord) -> bytes | None:
        cache_path = self.cache_dir / Path(cache_key(record))
        if cache_path.exists():
            with open(cache_path, "rb") as fp:
                logger.debug(f"reading data for {record.url} from filesystem cache")
                return fp.read()
        return None

    def set(self, record: DomainRecord, data: bytes) -> None:
        if not self.cache_dir.exists():
            logger.info(f"Creating cache dir {self.cache_dir}")
            os.makedirs(str(self.cache_dir))
        cache_path = self.cache_dir / Path(cache_key(record))
        with open(cache_path, "wb") as fp:
            logger.debug(f"writing data for {record.url} to filesystem cache")
            fp.write(data)

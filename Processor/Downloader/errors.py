from Aggregator.index_query import DomainRecord


class PageDownloadException(BaseException):
    def __init__(self, domain_record: DomainRecord, reason: str, status: int) -> None:
        super().__init__()
        self.domain_record = domain_record
        self.reason = reason
        self.status = status

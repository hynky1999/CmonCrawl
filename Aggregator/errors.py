class PageResponseError(BaseException):
    def __init__(
        self,
        domain: str,
        status: int,
        reason: str,
        cdx_server: str,
        *args: object,
        page: int = 0,
        retries: int = 0
    ) -> None:
        super().__init__(*args)
        self.domain = domain
        self.page = page
        self.cdx_server = cdx_server
        self.status = status
        self.reason = reason
        self.retires = retries


class LimitError(BaseException):
    def __init__(self, *args: object):
        super().__init__(*args)

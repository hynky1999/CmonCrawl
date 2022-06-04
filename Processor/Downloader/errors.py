class PageResponseError():
    def __init__(
        self, domain: str, status: str, reason: str, *args: object, page: int = 0
    ) -> None:
        super().__init__(*args)
        self.domain = domain
        self.page = page
        self.status = status
        self.reason = reason
class PageResponseError(BaseException):
    def __init__(self, CC_server: str, domain: str, page: int, *args: object) -> None:
        super().__init__(*args)
        self.CC_server = CC_server
        self.domain = domain
        self.page = page

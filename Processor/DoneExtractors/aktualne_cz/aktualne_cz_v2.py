from datetime import datetime
from DoneExtractors.aktualne_cz.aktualne_cz_v1 import AktualneCZV1Extractor


class AktualneCZV2Extractor(AktualneCZV1Extractor):
    SINCE = datetime(2015, 9, 10)
    TO = datetime(2019, 9, 10)

    def __init__(self):
        super().__init__()
        self.article_css_dict["content"] = "div.clanek"
        self.article_css_dict["category"] = "div.menu > ul > li > a.active"
        self.article_css_dict["brief"] = ".perex"
        self.article_css_selector = "body"
        self.date_css = ".titulek-pubtime"


extractor = AktualneCZV2Extractor()

from datetime import datetime
from DoneExtractors.aktualne_cz_old_old import AktualneCZOldOldExtractor


class AktualneCZOldExtractor(AktualneCZOldOldExtractor):
    SINCE = datetime(2015, 9, 10)
    TO = datetime(2019, 9, 10)

    def __init__(self):
        super().__init__()
        self.article_css_dict["content"] = "div.clanek"
        self.article_css_dict["category"] = "div.menu > ul > li > a.active"
        self.article_css_dict["brief"] = ".perex"
        self.article_css_selector = "body"
        self.date_css = ".titulek-pubtime"


extractor = AktualneCZOldExtractor()

from enum import Enum

from cmoncrawl.config import CONFIG
from cmoncrawl.processor.dao.api import CCAPIGatewayDAO
from cmoncrawl.processor.dao.s3 import S3Dao


class DAOname(Enum):
    S3 = "s3"
    API = "api"

    def __str__(self):
        return self.value


def get_dao(download_method: DAOname | None):
    match download_method:
        case DAOname.S3:
            return S3Dao(aws_profile=CONFIG.AWS_PROFILE)
        case DAOname.API:
            return CCAPIGatewayDAO()
        case None:
            return None

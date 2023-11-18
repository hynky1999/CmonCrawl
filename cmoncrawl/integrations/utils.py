from enum import Enum

from cmoncrawl.processor.dao.api import CCAPIGatewayDAO
from cmoncrawl.processor.dao.s3 import S3Dao
from cmoncrawl.config import CONFIG


class DAOname(Enum):
    S3 = "s3"
    API = "api"


def get_dao(download_method: DAOname | None):
    match download_method:
        case DAOname.S3:
            return S3Dao(aws_profile=CONFIG.AWS_PROFILE)
        case DAOname.API:
            return CCAPIGatewayDAO()
        case None:
            return None

from enum import Enum

from cmoncrawl.processor.connectors.api import CCAPIGatewayDAO
from cmoncrawl.processor.connectors.s3 import S3Dao


class DAOname(Enum):
    S3 = "s3"
    API = "api"


def get_dao(download_method: DAOname | None):
    match download_method:
        case DAOname.S3:
            return S3Dao()
        case DAOname.API:
            return CCAPIGatewayDAO()
        case None:
            return None

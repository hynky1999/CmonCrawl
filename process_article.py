import argparse
from pathlib import Path
import re
from Aggregator.index_query import DomainRecord
import asyncio
from Processor.OutStreamer.stream_to_file import (
    OutStreamerFileJSON,
)

from Processor.Router.router import Router
from Processor.utils import PipeMetadata

FOLDER = "articles_processed"


def parse_article(article_path: Path):
    with open(article_path, "r") as f:
        article = f.read()

    url = re.search(r'og:url" content="([^\s]+)"', article)
    if url is None:
        url = ""
    else:
        url = url.group(1)
    metadata = PipeMetadata(
        DomainRecord(
            filename=article_path.name,
            url=url,
            offset=0,
            length=100,
        ),
    )
    return article, metadata


async def article_process(article_path: Path, output_path: Path, router: Router):
    outstreamer = OutStreamerFileJSON(origin=output_path, pretty=True)
    article, metadata = parse_article(article_path)

    extractor = router.route(metadata.domain_record.url)
    output = extractor.extract(article, metadata)
    if output is None:
        return None
    path = await outstreamer.stream(output, metadata)
    print("Success")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download articles")
    parser.add_argument("article_path", type=Path)
    parser.add_argument("output_path", type=Path)
    args = parser.parse_args()

    # Router
    router = Router()
    router.load_modules(str(Path("UserDefinedExtractors").absolute()))
    router.register_route("aktualne_cz", [r".*aktualne\.cz.*"])
    router.register_route("idnes_cz", [r".*idnes\.cz.*"])
    router.register_route("seznamzpravy_cz", [r".*seznamzpravy\.cz.*"])
    asyncio.run(article_process(**vars(args), router=router))

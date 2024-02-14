.. _custom_pipeline:

How to extract from Common Crawl (practice)
===========================================

Since we now know what steps should we do in order to extract data from Common Crawl and
how they map to ``cmoncrawl`` primitives, let's now see how to do it in practice.


Pipeline
--------
We already know how to get the domain records and we also know how to download, extract and save the data.
The pipeline allows use to combine all but the first step into single object that can be used to extract data from Common Crawl.

To create a pipeline simply initialize :py:class:`cmoncrawl.processor.pipeline.pipeline.ProcessorPipeline` with Downloader, Router and Streamer.
You can then call it's :py:meth:`cmoncrawl.processor.pipeline.pipeline.ProcessorPipeline.process_domain_record` method with the query and it will run the whole pipeline for single domain record.


.. note::
    The exceptions are not handled by the pipeline and are passed to the caller, to handle them as you wish.

Simulatenous querying and extracting
------------------------------------

Now all we need to resolve is how t effectively connect querying index and download/extracting (pipeline) data.
One way is to query index and whenever we get a domain record, we can pass it to the pipeline, this is exactly how
:py:func:`cmoncrawl.integrations.middleware.synchronized.query_and_extract` works. This works great when we use Gateway DAO,
as the querying index takes about the same time as downloading/extracting. This is how we can do it:

.. code-block:: python
    :caption: Simultaneously query and extract data from Common Crawl

    from typing import Any, Dict
    from bs4 import BeautifulSoup
    from cmoncrawl.aggregator.gateway_query import GatewayAggregator
    from cmoncrawl.processor.pipeline.extractor import BaseExtractor
    from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline
    from cmoncrawl.processor.pipeline.downloader import AsyncDownloader
    from cmoncrawl.processor.pipeline.router import Router
    from cmoncrawl.processor.pipeline.streamer import StreamerFileJSON
    from cmoncrawl.common.loggers import all_purpose_logger
    from cmoncrawl.common.types import MatchType, PipeMetadata
    from cmoncrawl.middleware.synchronized import query_and_extract
    from cmoncrawl.processor.dao.s3 import S3Dao
    from pathlib import Path


    class YourCustomExtractor(BaseExtractor):
        def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> Dict[str, Any] | None:
            return {"title": "Dummy"}

    your_custom_extractor = YourCustomExtractor()

    # We register our custom extractor to the router
    router = Router()
    router.load_extractor("ext", your_custom_extractor)
    router.register_route("ext", ".*bbc.com.*")
    streamer = StreamerFileJSON(Path("extracted"), max_directory_size=1000, max_file_size=100)

    async with S3Dao(aws_profile="dev") as dao:
        downloader = AsyncDownloader(dao)
        pipeline = ProcessorPipeline(downloader=downloader, router=router, outstreamer=streamer)

        index_agg = GatewayAggregator(
            urls=["bbc.com"],
            match_type=MatchType.DOMAIN,
            limit=1000,
        )

        processed_urls = await query_and_extract(index_agg, pipeline)

Query records and then extract
------------------------------

The otherway is to query index for all records and download/extract them afterwards. This approach works
great with Athena as the query takes around 1-2 minutes. With this approach we can than abuse both multiprocessing to process
and asyncio queues to download the data faster. This is how we can do it:


.. code-block:: python
    :caption: Query and extract data from Common Crawl

    from cmoncrawl.aggregator.athena_query import AthenaAggregator
    from cmoncrawl.common.types import MatchType
    from typing import Any, Dict
    from bs4 import BeautifulSoup
    from cmoncrawl.aggregator.gateway_query import GatewayAggregator
    from cmoncrawl.processor.pipeline.extractor import BaseExtractor
    from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline
    from cmoncrawl.processor.pipeline.downloader import AsyncDownloader
    from cmoncrawl.processor.pipeline.router import Router
    from cmoncrawl.processor.pipeline.streamer import StreamerFileJSON
    from cmoncrawl.common.loggers import all_purpose_logger
    from cmoncrawl.common.types import MatchType, PipeMetadata
    from cmoncrawl.middleware.synchronized import extract
    from cmoncrawl.processor.dao.s3 import S3Dao
    from pathlib import Path

    # Query
    records = []
    async with AthenaAggregator(urls=["bbc.com"],
        match_type=MatchType.DOMAIN,
        limit=1000,
        bucket_name="test-dev-cmoncrawl",
        aws_profile="dev"
    ) as agg:
        async for record in agg:
            records.append(record)

    #Then extract



    class YourCustomExtractor(BaseExtractor):
        def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> Dict[str, Any] | None:
            return {"title": "Dummy"}

    your_custom_extractor = YourCustomExtractor()

    # We register our custom extractor to the router
    router = Router()
    router.load_extractor("ext", your_custom_extractor)
    router.register_route("ext", ".*bbc.com.*")
    streamer = StreamerFileJSON(Path("extracted"), max_directory_size=1000, max_file_size=100)

    async with S3Dao(aws_profile="dev") as dao:
        downloader = AsyncDownloader(dao)
        pipeline = ProcessorPipeline(downloader=downloader, router=router, outstreamer=streamer)

        index_agg = GatewayAggregator(
            urls=["bbc.com"],
            match_type=MatchType.DOMAIN,
            limit=1000,
        )

        processed_urls = await extract(pipeline=pipeline, records=[(rec, {}) for rec in records])

To leverage multiprocessing, simply divide the records into n chunks and for each chunk initialize a new process.

Distributed Simulatenous high-throughput querying and extracting
----------------------------------------------------------------

Lastly you can leverage :py:class:`cmoncrawl.middleware.stompware.StompAggregator` to query and send data to queue using stomp protocol,
and simulatenous retrieve the data from the queue and extract it using :py:class:`cmoncrawl.middleware.stompware.StompProcessor`.


Be cooperative
--------------
If you plan to use multiprocessing or distributed approach, please try to be nice to others and limit the number of requests
at Downloader/Aggregator accordingly.
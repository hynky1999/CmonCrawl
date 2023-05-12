.. _custom_pipeline:

Custom Pipeline
===============



Pipeline
--------

The pipeline then all but querying together.
To create a pipeline simply initialize :py:class:`cmoncrawl.processor.pipeline.pipeline.ProcessorPipeline` with Downloader, Router and Streamer.
You can then call it's :py:meth:`cmoncrawl.processor.pipeline.pipeline.ProcessorPipeline.process_domain_record` method with the query and it will run the whole pipeline for single domain record.


.. note::
    The exceptions are not handled by the pipeline and are passed to the caller, to handle them as you wish.

Putting it all together
-----------------------

We now show how to create very simple custom pipeline that will download and extract 
data into json programmatically.


.. code-block:: python
    :caption: Using the lib in code

    from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline
    from cmoncrawl.processor.pipeline.downloader import AsyncDownloader
    from cmoncrawl.processor.pipeline.router import Router
    from cmoncrawl.processor.pipeline.streamer import StreamerFileJSON
    from cmoncrawl.common.loggers import all_purpose_logger
    from cmoncrawl.common.types import MatchType
    from commoncrawl.integrations.middleware.synchronized import query_and_extract
    from pathlib import Path

    downloader = AsyncDownloader()

    your_custom_extractor = YourCustomExtractor()
    router = Router()
    router.load_extractor("ext", your_custom_extractor)
    router.register_route("ext", ".*bbc.com.*")
    streamer = StreamerFileJSON(Path("extracted"))
    pipeline = ProcessorPipeline(downloader, router, streamer)

    index_agg = IndexAggregator(
        domains=["bbc.com"],
        match_type=MatchType.DOMAIN,
        limit=1000,
    )

    processed_urls = await query_and_extract(index_agg, pipeline)

The code will try to extract first 1000 pages from bbc.com, which will
be extracted using YourCustomExtractor and save the results to json files.
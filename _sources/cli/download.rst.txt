Command Line Download
=====================

The download mode of the ``cmon`` command line tool serves to query and download from CommonCrawl indexes.
The following arguments are needed in this order:

Positional arguments
--------------------

1. output - Path to output directory.

2. {record,html} - Download mode:

   - record: Download record files from Common Crawl.
   - html: Download HTML files from Common Crawl.

3. urls - URLs to download, e.g. www.bcc.cz.


In html mode, the output directory will contain .html files, one
for each found URL. In record mode, the output directory will contain
``.jsonl`` files, each containing multiple domain records in JSON format.


Options
-------

--limit LIMIT
   Max number of URLs to download.

--since SINCE
   Start date in ISO format (e.g., 2020-01-01).

--to TO
   End date in ISO format (e.g., 2020-01-01).

--cc_server CC_SERVER
   Common Crawl indexes to query. Must provide the whole URL (e.g., https://index.commoncrawl.org/CC-MAIN-2023-14-index).

--max_retry MAX_RETRY
   Max number of retries for a request. Increase this number when requests are failing.

--sleep_base SLEEP_BASE
   Base sleep time for exponential backoff in case of request failure.

--max_requests_per_second MAX_REQUESTS_PER_SECOND
   Max number of requests per second.

--match_type MATCH_TYPE
   One of exact, prefix, host, domain
   Match type for the URL. Refer to cdx-api for more information.
   See :py:class:`cmoncrawl.common.types.MatchType` for more information.

--max_directory_size MAX_DIRECTORY_SIZE
   Max number of files per directory.

--filter_non_200
   Filter out non-200 status code.
   
--aggregator AGGREGATOR
   Aggregator to use for the query.

   - athena: Athena aggregator. Fastest, but requires AWS credentials with correct permissions. See :ref:`misc/athena:Athena` for more information.
   - gateway: Gateway aggregator (default). Very slow, but no need for AWS config.

--s3_bucket S3_BUCKET
   S3 bucket to use for Athena aggregator. Only needed if using Athena aggregator.

   - If set the bucket will not be deleted after the query is done, allowing to reuse it for future queries.
   - If not set, a temporary bucket will be created and deleted after the query is done.

.. note::
   If you specify an S3 bucket, remember to delete it manually after you're done to avoid incurring unnecessary costs.


Record mode options
-------------------

--max_crawls_per_file MAX_CRAWLS_PER_FILE
    Max number of domain records per file output

HTML mode options
-----------------

--encoding ENCODING
   Force usage of specified encoding if possible.

--download_method DOWNLOAD_METHOD
   Method for downloading warc files from Common Crawl, it only applies to HTML download.

   - api: Download from Common Crawl API Gateway. This is the default option.
   - s3: Download from Common Crawl S3 bucket. This is the fastest option, but requires AWS credentials with correct permissions.


Examples
--------


.. code-block:: bash

    # Download first 1000 domain records for example.com
    cmon download dr_output record --match_type=domain --limit=1000 example.com

    # Download first 100 htmls for example.com
    cmon download html_output html --match_type=domain --limit=100 example.com

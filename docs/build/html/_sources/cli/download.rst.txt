Command Line Download
=====================

The download mode of the ```cmon``` command line tool servers to query and download from CommonCrawl indexes.
The following arguments are needed in this order:

Positional arguments
--------------------

1. url - URL to query.

2. output - Path to output directory.

3. {record,html} - Download mode:

   - record: Download record files from Common Crawl.
   - html: Download HTML files from Common Crawl.


In html mode, the output directory will contain .html files, one
for each found URL. In record mode, the output directory will contain
```.jsonl``` files, each containing multiple domain records in JSON format.


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

--sleep_step SLEEP_STEP
   Number of additional seconds to add to the sleep time between each failed download attempt. Increase this number if the server tells you to slow down.

--match_type MATCH_TYPE
   One of exact, prefix, host, domain
   Match type for the URL. Refer to cdx-api for more information.

--max_directory_size MAX_DIRECTORY_SIZE
   Max number of files per directory.

--filter_non_200
   Filter out non-200 status code.

Record mode options
-------------------

--max_crawls_per_file MAX_CRAWLS_PER_FILE
    Max number of domain records per file output



Examples
--------


.. code-block:: bash

    # Download first 1000 domain records for example.com
    cmon download --match_type=domain --limit=1000 example.com dr_output record

    # Download first 100 htmls for example.com
    cmon download --match_type=domain --limit=100 example.com html_output html
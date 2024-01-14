Command line Extract
====================

The extract mode of the ``cmon`` command line tool serves to extract data from your downloaded files.
The following arguments are needed in this order:

Positional arguments
--------------------

1. config_path - Path to the config file containing extraction rules.

2. output_path - Path to the output directory.

3. {record,html} - Extraction mode:

   - record: Extract data from jsonl (domain record) files.
   - html: Extract data from HTML files.

4. files - Files to extract data from (Either HTML files or .jsonl files).

To create a config file, see :ref:`extractor_config`.

Both modes yield the same output format, which is a ``.jsonl`` file containing the extracted data,
one per line. For each file, a new directory is created in the output directory, named after the
file.

The files created by the download mode can be directly used with the appropriate mode
in the extraction.

- If you have an HTML file, you can use the HTML mode to extract it.
- If you have a domain records, you can use the RECORD mode to extract it.
- If you have domain records, which you acquired without using cmoncrawl, 

please refer to :ref:`domain_record_jsonl`, which describes how to create ``.jsonl`` files from your domain records,
which you can then use with the record mode.

Optional arguments
------------------

--max_crawls_per_file MAX_CRAWLS_PER_FILE
   Max number of extractions per file output.

--max_directory_size MAX_DIRECTORY_SIZE
   Max number of extraction files per directory.

--n_proc N_PROC
   Number of processes to use for extraction. The parallelization is on file level,
   thus for a single file, it's useless to use more than one process.

Record arguments
----------------

--max_retry MAX_RETRY
   Max number of WARC download attempts.

--download_method DOWNLOAD_METHOD
   Method for downloading warc files from Common Crawl, it only applies to HTML download.

   - api: Download from Common Crawl API Gateway. This is the default option.
   - s3: Download from Common Crawl S3 bucket. This is the fastest option, but requires AWS credentials with correct permissions.

--sleep_base SLEEP_BASE
   Base value for exponential backoff between failed requests.

--max_requests_per_second MAX_REQUESTS_PER_SECOND
   Max number of requests per second.

Html arguments
--------------

--date DATE
   Date of extraction of HTML files in ISO format (e.g., 2021-01-01). The default is today.

--url URL
   URL from which the HTML files were downloaded. By default, it will try to infer from the file content.

Examples
--------

.. code-block:: bash

    # Take the domain records downloaded using the first command and extracts them using your extractors
    cmon extract config.json extracted_output dr_output/*.jsonl record --max_retry 100 --download_method=gateway --sleep_base 1.3 

    # Take the htmls downloaded using the second command and extracts them using your extractors
    cmon extract config.json extracted_output html_output/*.html html --date 2021-01-01 --url https://www.example.com

When you are going to build the extractors, you will appreciate that you can specify
what the URL of the HTML file is and what the date of the extraction is. This is because 
those information are used during the extractor routing.
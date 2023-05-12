Command line Extract
====================

The extract mode of the ```cmon``` command line tool servers to extract your download files.
The following arguments are needed in this order:

Positional arguments
--------------------


1. config_path - Path to config file containing extraction rules.

2. output_path - Path to output directory.

3. files - Files to extract data from.

4. {record,html} - Extraction mode:

   - record: Extract data from jsonl (domain record) files.
   - html: Extract data from HTML files.

To create a config file, see :ref:`extractor_config`.

Both modes yield the same output format, which is a ```.jsonl``` file containing the extracted data,
one per line. For each file a new directory is created in the output directory, named after the
file.

The files created by the download mode, can be directly used with appropriate mode
in the extraction. If you have an html file, you can use the html mode to extract it.
If you have a domain records, which you got some other way (AWS Athena), please refer to :ref:`domain_record_jsonl`,
which describes how to create ```.jsonl``` files from your domain records, which you can then
use with the record mode.





Optional arguments
------------------

--max_crawls_per_file MAX_CRAWLS_PER_FILE
   Max number of extractions per file output.

--max_directory_size MAX_DIRECTORY_SIZE
   Max number of extraction files per directory.

--n_proc N_PROC
   Number of processes to use for extraction. The paralelization is on file level,
   thus for single file it's useless to use more than one process.

Record arguments
----------------

--max_retry MAX_RETRY
   Max number of WARC download attempts.

--sleep_step SLEEP_STEP
   Number of additional seconds to add to the sleep time between each failed download attempt.

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
    cmon extract config.json extracted_output dr_output/*/*.jsonl record --max_retry 100 --sleep_step 10

    # Take the htmls downloaded using the second command and extracts them using your extractors
    cmon extract config.json extracted_output html_output/*/*.html html --date 2021-01-01 --url https://www.example.com


When you are going to build the extractors, you gonna appreaciate that you can specify
what the url of the html file is and what the date of the extraction is. This is because 
those information are used during the extractor routing.
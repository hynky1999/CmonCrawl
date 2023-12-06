.. _cli:

Command Line Interface
======================

The command line interface is a simple wrapper around the library.

It provides the two main functionalities:

* `download` - Downloads samples of either :ref:`domain_record` or HTML from common crawl indexes
* `extract` - Downloads an HTML from Domain Record and extracts the content. It can also directly take the HTML and extract the data.

Both functionalities are invoked using ``cmon`` followed by the functionality and the required arguments.
The ``cmon`` command also takes a few optional arguments:

--verbosity
   Verbosity level. Choices are [0, 1, 2], with 0 being the least verbose and 2 being the most verbose. Default is 1.

--aws_profile
   AWS profile to use for AWS calls (Athena, S3). If not provided, the default AWS profile will be used.

Examples
--------

.. code-block:: bash

    # Download first 1000 domain records for example.com
    cmon download --match_type=domain --limit=1000 dr_output record example.com

    # Download first 100 htmls for example.com
    cmon download --match_type=domain --limit=100 html_output html example.com

    # Take the domain records downloaded using the first command and extracts them using your extractors
    cmon extract config.json extracted_output dr_output/*.jsonl record

    # Take the htmls downloaded using the second command and extracts them using your extractors
    cmon extract config.json extracted_output html_output/*.html html












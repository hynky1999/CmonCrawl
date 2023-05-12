Usage
=====

The library is designed to make interaction with CommonCrawl's indexes simple,
while also providing a framework for extracting data from the downloaded 
HTMLs.

You can use the library in two ways:

1. :ref:`cli` - This should suffice for 99% of the use cases.
2. :ref:`custom_pipeline` - If you need more control over the process, you can use the library programmatically.



Workflow
--------

The workflow is two-step:

1. First download domain records (see :ref:`domain_record`) from the indexes.
2. Extract the domain records.

.. note::
    This will further allow you to share the domain records with others,
    so that you will not run into author law issues.

.. note::
    First step can be skipped by using AWS Athena, which is under 
    current cirmustances (CommonCrawl api is completely throttled, slow and dropping most of requests),
    the prefered way. See `How to get records from AWS Athena <https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/>`_.
    It's also super cheap.


To create your custom extractors you likely want to download HTMLs not domain records.
Both download to HTML and extraction from HTML is also supported in parallel to the domain record workflow.
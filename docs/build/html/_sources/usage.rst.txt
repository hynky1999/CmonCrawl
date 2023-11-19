Usage
=====

The library is designed to make interaction with CommonCrawl's indexes simple,
while also providing a framework for extracting data from the downloaded 
HTMLs.

You can use the library in two ways:

1. :ref:`cli` - This should suffice for 80% of the use cases. Restricted, but easy to use.
2. :ref:`custom_pipeline` - If you need more control over the process, you can use the library programmatically.

Workflow
--------
In order to download from CommonCrawl you first need to find the pointers to the data you want to download.
Search for the pointers is done over the specific files called indexes. The indexes don't contain the data itself,
but rather metadata and pointers to the data. We call these pointers domain records (see :ref:`domain_record`).
Once you have the domain records you can download the data from the CommonCrawl's S3 bucket. Since you might want
to extract only specific data from the downloaded HTMLs, you can also specify a list of extractors to be run on the
downloaded HTMLs.

The library thus supports the two step workflow:

1. First download domain records from the indexes.
2. Download and extract the domain records.

AWS
---
The CommonCrawl are stored on AWS S3 us-east-1 bucket. The CommonCrawl allows you to access the data using following methods:

1. Gateway - you can download the data throught CloudFlare HTTP Gateway. You will not need AWS credentials, but it is also the slowest.
2. S3 - you can download the data directly from S3. You will need AWS credentials, but it is also the fastest.

Additionaly, the CommonCrawl provides two ways to to query the data:

1. CommonCrawl Index - Free, but more limited and incrdibly slow.
2. AWS Athena - Paid, but much faster, you can use SQL to query the data.

The library supports all of these methods. We recommend using S3/AWS Athena combination. Refer to the image referenced as :ref:`when_to_use_this_library` for more details.

.. _when_to_use_this_library:

.. image:: ../source/images/when_to_use.drawio.png
   :alt: When to use this library

Be nice to others
-----------------
If you use the library programmatically or through CLI,
you will find, that you can specify the number of threads to use.
Please be aware that by default we limit the number of requests per thread
to 20/s. This is to prevent overloading the CommonCrawl's servers. If you
plan to use more threads, be considerate to others and don't set the number
of threads too high.
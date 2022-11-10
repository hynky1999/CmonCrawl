Quick Overview
==============

The process of getting one parsed web page from CommonCrawl can be described as a pipeline.

1. Query CommmonCrawl to find a link to a file that contains the web page we want.
2. Download a file
3. Choose parser for the web page
4. Filter out the web page if not matching the conditions
5. Extract fields from the page
6. Save the fields to a file



The first step is handled by `Aggregator` while the rest is handled by `Processor`.
We will now go through each step in detail. We will be describing the usage where we use the Artemis Queue as middleware between the `Aggregator` and `Processor` to communicate.
However it should not be much different with other middleware of your choice. Please refere to `download_article.py` which should give you an idea how to create a custom pipeline where you can insert your own middleware.



=======================
1. Querying CommonCrawl
=======================
what WARC File how
    `WARC <https://en.wikipedia.org/wiki/Web_ARChive>`_ is a file format that is used for storing multitudes of web resources.
    In our case these files contain a bunch of downloaded web pages and their metadata.
    It's possible to get only part of the file by specifying the offset in file and length of the part we want.


what
    Common Crawl Index
how
    A CommonCrawl index is a collection which maps crawled urls to WARC file which contain the crawl of that url.

Every month a CommonCrawl releases a new index which contains all links to web pages that were crawled that month.

.. warning::
    It is important to understand that even if the index was released in a certain month, it can contain the links to web pages that might be older.

Thus in order to download an page we query the index to get link to respective WARC file, offset and length of page.
Since there are multiples of the indexes we should query all of them to make sure we don't miss the page.
With the link to the WARC and offset and length we can continue to another step. 

All this is handled by :py:class:`Aggregator.App.index_query.IndexAggregator`. But for basic use you will not need to use it directly.


The :py:mod:`Aggregator.aggregator` is the file you will work with.
It's command line utility which will start and instance of aggregator. Such an instance will query the specified indexes with given url and send the results to the Middleware(Artemis Queue).


.. code-block:: bash
    :caption: This will start and instance of aggregator which will query the indexes from 2014-12-30 to 2016-01-30 for the url bbc.com

        $ python -m  Aggregator.aggregator --url bbc.com --since=2014-12-30 --to=2016-01-30




=====================
2. Downloading a file
=====================
The Processor node than downloads the url and related information from queue and downloads the appropriate WARC file.
This step is handled by :py:mod:`Processor.App.Downloader.downloader.Downloader`.
It simply downloads and extracts the page from the WARC file.


================
3. Choose parser
================

Once the page is downloaded we first need to choose a parser for it.
Parsers are dynamically loaded based on definitions in config file. By default such a config
is `Processor/config.json`.  All loaded processors are then matched against the url and publication date and first matching is used.
This functionality is handled by :py:class:`Processor.App.Router.router.Router`.


=============================
4. Filtering out the web page
=============================

Once the parser is chosen the filtering function defined by the extractor is used to either drop or pass a page.

===============================
5. Extract fields from the page
===============================

The extracting function defined by the extractor is used to extract the fields from the page.
The extracting rules can be defined in 2 ways.

1. Using parsed version of the page (BeautifulSoup) and the extracting the respected fields yourself.
2. Using predefined transfomations. 

Using first method should be straightforward. Just extract the values and return them in dict.

To use transformations we first need to define what html tags we want to extract from head and body respectively.
Then for each such a tag we need to provide a list of transformations. These transformations then run in sequence where previous result is passed to the next one as parameter.
Whenever the None is encountered the transformation will not raise and error but will not continue and set respective field to None.
You can think about it as a composition of functions.

When we were developing our extractors we found the second approached to be much more readable and easier to use
because it's very obnoxious to handle the None values with complex logic.


==============
6. File saving
==============
With the field extracted we need to save them to a file.
By default the fields are saved in json file.
The way the file is saved is defined by outstreamers. We have implemented the json outstreamer
and field per line outstreamer
:py:class:`Processor.App.OutStreamer.stream_to_file.OutStreamerFileJSON` and :py:class:`Processor.App.OutStreamer.stream_to_file.OutStreamerFileDefault` respectively.

If you would like different format you can create your own saver by inheriting from :py:class:`Processor.App.OutStreamer.outstreamer.OutStreamer` and then changing pipeline creation with your new outstreamer.
Programming Guide
==========================

This section provides a brief overview of the project. It should give you
and idea of how to create your custom extraction pipeline.

.. note:
    You probably don't need to read this if you just want to use the utility.
    This is for people who want to create their own extraction pipeline.


How to extract from Common Crawl (theory)
=========================================

The process of getting one parsed web page from CommonCrawl can be described as a pipeline.

1. Query CommmonCrawl to find a link to a file that contains the web page we want.
2. Download a file
3. Choose parser for the web page
4. Filter out the web page if not matching the conditions
5. Extract fields from the page
6. Save the fields to a file



The first step is handled by `Aggregator` while the rest is handled by `Processor`.

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

All this is handled by :py:class:`cmoncrawl.aggregator.index_query.IndexAggregator`. But for basic use you will not need to use it directly.


=====================
2. Downloading a file
=====================
The Processor node than downloads the url and related information from queue and downloads the appropriate WARC file.
This step is handled by :py:mod:`cmoncrawl.processor.pipeline.downloader.AsyncDownloader`.
It simply downloads and extracts the page from the WARC file.


===================
3. Choose extractor
===================
Once the page is downloaded we first need to choose a extractor for it.
Extractors are dynamically loaded based on definitions in :ref:`extractor_config`.
All loaded processors are then matched against the url and crawl date and first matching is used.
This functionality is handled by :py:class:`cmoncrawl.processor.pipeline.router.Router`.

For development of extractors refer to :ref:`extractors`.


=============================
4. Filtering out the web page
=============================

Once the extractor is chosen the filtering function is used to either drop or pass a page.
In order to filter your you can use either :py:meth:`cmoncrawl.processor.pipeline.extractor.BaseExtractor.filter_raw` for
filtering based on raw html pages (fast). Or wait for conversion to soup and then filter using
:py:meth:`cmoncrawl.processor.pipeline.extractor.BaseExtractor.filter_soup` (slow).

===============================
5. Extract fields from the page
===============================

The extracting function defined by the extractor is used to extract the fields from the page.
Just extract the values and return them in dict.


==============
6. File saving
==============
With the field extracted we need to save them to a file.
By default the fields are saved in json file.
The way the file is saved is defined by streamers.
All of the currently implemented streamers are derived from :py:class:`cmoncrawl.processor.pipeline.streamer.BaseStreamerFile`.
Which defined how are the files saved, but the content parsing is left to the derived classes.

Currently we support 2 streamers, one for json (:py:class:`cmoncrawl.processor.pipeline.streamer.StreamerFileJSON`) and one for html (:py:class:`cmoncrawl.processor.pipeline.streamer.StreamerFileHTML`).
The json one creates a json per line output, and outputs all extracted data.
The html one creates a html file (assuming the html is defined in extracted data['html']).

If you would like different format you can create your own saver by inheriting from :py:class:`cmoncrawl.processor.pipeline.streamer.IStreamer` and then changing pipeline creation with your new outstreamer.
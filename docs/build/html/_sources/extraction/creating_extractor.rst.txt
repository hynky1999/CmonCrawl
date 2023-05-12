.. _extractors:

Custom Extractor
====================


All the extractors that are used within CLI must implement the :py:class:`cmoncrawl.processor.pipeline.extractor.IExtractor` class.

However for most of the use cases, you will use the :py:class:`cmoncrawl.processor.pipeline.extractor.BaseExtractor` class.

BaseExtractor
-------------

The `BaseExtractor`` assumes you will want to use parsed HTML using
`BeautifulSoup <https://www.crummy.com/software/BeautifulSoup/bs4/doc/>`_. Thus the only method you need to implement is the `extract_soup` method.


Extraction
----------

- `extract_soup` method
It takes a BeautifulSoup object and crawl metadata (see :py:class:`cmoncrawl.common.types.PipeMetadata`) and must return
a dictionary of extracted data or None if the page should not be extacted, for example if you haven't found all the data you need.

Additionaly, you might want to filter the pages you don't want to
extract. For this, you have two options:

Filtering
---------

- `filter_raw` method
This method take the raw HTML and crawl metadata and must return True if the page should be extracted or False otherwise. If you can
decide based on raw HTML, this is the most efficient way to filter pages, as now soup parsing will be done.

- `filter_soup` method
This method take the BeautifulSoup object and crawl metadata and must return True if the page should be extracted or False otherwise.


Finally your file must create the said extractor and name it `extractor`.

You can also set `NAME` variable to a string that will be used to name the extractor, otherwise the name of file without extension will be used.

Example
-------

Here is an example of an extractor that will extract the title of the page.

.. code-block:: python
    :caption: ext.py


    from cmoncrawl.processor.pipeline.extractor import BaseExtractor
    from cmoncrawl.common.types import PipeMetadata

    class TitleExtractor(BaseExtractor):
        def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> dict:
            return {'title': soup.title.text}

        def filter_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> bool:
            return soup.title is not None

    extractor = TitleExtractor()
    NAME='title'


Now in :ref:`config_file` you would refer to this extractor as `title`.
If you would't set the `NAME` variable, you would refer to it as `ext`.
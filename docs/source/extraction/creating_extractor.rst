.. _extractors:

Extractor types
================

All the extractors you will write must implement the :py:class:`cmoncrawl.processor.pipeline.extractor.IExtractor` class.
If you choose to implement it directly, you will have to implement the ``extract`` method.
In the method you are provided with the HTML page as a string and crawl Medatata. You then define what data you want to extract from HTML as dictionary or None if you want
to discard the HTML.

While the interface is simple it doesn't handle encoding problems or filtering.
If you want to parse the HTML using ``bs4`` and then extract the data you can use either:

- :py:class:`cmoncrawl.processor.pipeline.extractor.BaseExtractor`, which parses the HTML using ``bs4`` and resolves encoding issues
- :py:class:`cmoncrawl.processor.pipeline.extractor.PageExtractor`, in which you just define CSS selectors to use and function which transform the data from selectors

Extractor Definition
====================
In order to register you extractor, you must define each extractor in
separate file and you must initialize the extractor in that file to variable
named `extractor`.

Example 1.
----------

.. code-block:: python
   :caption: extractor.py

   # You can either use the NAME variable to define name,
   # otherwise the name will be inherited from the file name
   NAME='title_extractor'

   from cmoncrawl.processor.pipeline.extractor import IExtractor
   from cmoncrawl.common.types import PipeMetadata

   class MyExtractor(IExtractor):
       def extract(self, response: str, metadata: PipeMetadata) -> Dict[str, Any] | None:
           return {"title": "My title"}

   extractor = MyExtractor()


BaseExtractor
=============

The `BaseExtractor`` assumes you will want to use parsed HTML using
`BeautifulSoup <https://www.crummy.com/software/BeautifulSoup/bs4/doc/>`_
Thus the only method you need to implement is the `extract_soup` method.

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

Example 2.
----------

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

Now in :ref:`extractor_config` you would refer to this extractor as `title_extractor`.
If you would't set the `NAME` variable, you would refer to it as `ext`.
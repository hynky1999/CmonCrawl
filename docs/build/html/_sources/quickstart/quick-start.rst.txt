Quickstart
==========

In this chapter we will show how to use the program to fetch a data from an url.
We will show this by an example.


=========
Extractor
=========

We would like to fetch all data from bbc.com containing the word "war" fetched since 20.1.2021 to 20.3.2021.

.. note:: It's important to emphasis the program can only fetch data based on crawl time. There is no way for program to know when was article published. It's possible to first find the published date and then drop all articles that are out of range. However all since/to dates are based on crawl time NOT published time.


For such pages we would like to extract the title and the content of the page.

Having our task established we will first have to write an extractor for the bbc pages.
This is done by creating a class that extends the class :py:class:`Processor.App.ArticleUtils.article_extractor.ArticleExtractor`.
We will create this class in the file :file:`Processor/UserDefined/Extractors/bbc_extractor.py`.




.. code-block:: python

    from Processor.App.ArticleUtils.article_extractor import ArticleExtractor
    from datetime import datetime

    class BBCExtractor(ArticleExtractor):
        SINCE = datetime(2021, 1 , 20)
        TO = datetime(2021, 3, 20)

        def __init__(self):
            pass

    extractor = BBCExtractor()


As you can see we have also created and instance which is required as it allows parametric constructors for extractors.


=====================
`download_article.py`
=====================

But what now ? We have no idea how the bcc site looked at the time of extracting.
That's is why the :py:mod:`download_article` exists!
Thus we run:

.. code-block:: bash
   :caption: This will download 1000 articles from bbc.com and save them to the directory `out1`.
    
    $ python download_article.py --since=2021-01-20 --to=2021-03-20 --limit=1000 bbc.com out1

Bear in mind that it will take some time to download all the pages.
It's possible that it will download some pages without any content but it's impossible to filter them.

.. note:: In my case it didn't download any english articles only ones in chinese and arabic. However it shouldn't matter as we only care about structure. If you want english articles just raise up the limit and wait.

Once downloaded we can inspect the pages in a browser.
We can see that title can be found in `h1#content` tag.
The article content can be found under `main[role=main]` tag and
the text is mostly in <p> tags(Usually you want to be more precise with this be we assume this for simplicity).

.. warning:: Always make sure that the tags you found are unique. Cross-check this with other articles fetched.

============================
Extracting (Transformations)
============================


With this information we can write the extractor.

.. code-block:: python


    from Processor.App.ArticleUtils.article_utils import aritcle_content_transform, headline_transform, get_text_transform
    REQUIRED_FIELDS = {
        "title": False,
        "content": True
    }
    
    def content_transform(soup):
        return [p.text for p in soup.find_all("p", recursive=True)]
    

    def __init__(self):
        super().__init__(
            article_css_dict={
                "title": "h1#content",
                "content": "main[role=main]",
            },
            # Here we define how to transform the content of the tag into a string.
            article_extract_dict= {
                "title": [get_text_transform, headline_transform],
                "content": [content_transform, text_unifications_transform, lambda lines : "\n".join(lines)]
            },


            # Here we define how to bind a tag that containt all fields we will use in article_css_dict
            # If you don't know just use body
            article_css_selector="body",
            required_fields=REQUIRED_FIELDS,
            non_empty = True
        )


`REQUIRED_FIELDS` is a dictionary that defines which fields must be extracted (Must be contained in resulting dictionary).
This is useful if you write multiple extractors and you want to make sure that all of them contain the same fields.
As you can see we have set the title to False this means that it's value can be None. We have set article to False which
means that is must not be None value. Because we have set non_empty to True the title also cannot be empty string or empty list.

`article_css_dict` define where to find the title and content.
`article_extract_dict` defines how to extract the title and content from the tag.
We have used some predfedined function from :py:mod:`Processor.App.ArticleUtils.article_utils` to help us with this.
Please look to the :py:mod:`Processor.App.ArticleUtils.article_utils` to check what exactly the transformations do! Should be clear from the code.
For content we created our transform which returns a list of text in p. 


`article_css_selector` simply defines where to start looking for the tags defined in `article_css_dict`.

.. note:: `header_css_dict` and `header_extract_dict` can also be set in constructor for extracting from html <head> tag. The get_attribute method is used to extract the attribute value.


========================
Extracting( BS4 version)
========================

Now the extracting part is finished. If it feels too complicated then you can you BeautifulSoup approach.
In that case you don't set `article_css_dict` and `article_extract_dict` but you have to implement the :py:meth:`Processor.App.ArticleUtils.article_extractor.ArticleExtractor.custom_extract` method.

.. code-block:: python

        def custom_extract(self, soup, metadata):
            # Find the field with the title
            title = soup.find("div", {"class": "title"})
            title_text = title.text if title else None
            return {"title": title_text}

In this case you can also access metadata from the warc file!

=========
Filtering
=========


We almost forgot that we want to filter the articles by the word "war".
We just need to override the :py:meth:`Processor.App.ArticleUtils.article_extractor.ArticleExtractor.custom_filter_soup` method.
Let's do it now!


.. code-block:: python

        def custom_filter_soup(self, soup, metadata):
            result = soup.find_all("p", lambda tag: "war" in tag.text)
            if result:
                return True
            return False

.. note:: You can also use the :py:meth:`Processor.App.ArticleUtils.article_extractor.ArticleExtractor.custom_filter_raw` which take the raw html as a parameter. It's usefull if don't need parsed html as processing is faster.

.. note:: This is where you could filter by date. metadata is instance of :py:class:`Processor.App.processor_utils.PipeMetadata` and it has warc_header property. You could get `Last-Modifier <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified>`_ from it but it's no guarantee that is published date. Or you could find it in parsed html.


===========
config.json
===========
To register our extractor create :file:`Processor/UserDefined/config.json` as:

.. code-block:: json

    {
        "addresses": [ "queue.bbc.com", ],
        "extractors_path": "./Extractors"
        "routes":[
            {
                "regexes": [".*bbc\\.com.*"],
                "extractors": ["bbc_extractor.py"]
            },
    }


This defines three things:

1. The address of the queue. This is tell artemis that we want to accept message from bbc.com queue. Basically always set this to "queue.{your domain}".

2. The path to the extractors (w.r.t config.json location)

3. The routes. This defines which extractor to use for which url. In this case we want to use bbc_extractor.py for all urls that contain bbc.com.

=====================
Testing our extractor
=====================

Before we run the extractor we should test it.
That is why we have the :py:mod:`Processsor.process_article` !

.. code-block:: bash
    :caption: This will process the article and output result to out2 folder if succesful.

    $ python -m Processor.process_article --date=2021-02-01 --config=Processor/UserDefined/config.json out1/directory_1/371_https\:__www.bbc.com_yoruba_afrika-44296108.html out2


.. note:: We have to use date as the extractor has no idea when the article was fetched because we have no infromation from warc anymore. You can also use url to specify the url of the article as again we don't have that infromation from warc. However in most cases it will correctly guess the url.


=====================
Running the extractor
=====================
We are now ready to run the extractor!
You can manually run the extractor node, aggregator node and artemis queue.
However it's not really convenient. And also requires some knowledge how to run atemis queue.
We thus added support for docker.
First we need to create :file:`docker-compose.yml`.

.. code-block:: bash

    version: "3.9"
    services:
    # Creates the artemis queue
    artemis:
        build: "./Artemis"
        container_name: "artemis"
        # Persistent volume
        # Make sure you prune to get clean state for test runs
        - artemis-data:/var/lib/artemis/data
        ports:
        - "8161:8161"
        # You can set up limits here
        # But make sure you also correct java memory in artemis Dockerfile in order to have effect.
        deploy:
        resources:
            reservations:
            memory: 8g

    producer-bbc.com:
        # This is the producer service, you can have multiple for different urls
        # Will run the aggregator part as you can see we set the date range
        build: ./Aggregator
        command: [ "--to=2021-03-20", "--since=2021-01-20", "bbc.com" ]
        depends_on:
        - consumer

    # This is consumer spawn as many as you want, you ideally want to have pills set up to number of producers.
    # Make sure you use use-hostname-output to have different output folders for each consumer.
    consumer:
        build: ./Processor
        command:
        [
            "--use_hostname_output",
            "--timeout=1",
            "--pills_to_die=1",
            "--queue_size=200"
        ]
        volumes:
        - ./output:/output:z
        deploy:
        # Number of replicas, more = faster processing
        replicas: 4
        depends_on:
        - artemis

    volumes:
        artemis-data:


This one is a bit more complex. It's standard docker-compose file so if you have experience with docker it sould be familiar.
It defines three services:

1. Artemis queue - This is the queue that will be used to communicate between the aggregator and the processor.  As one of the goal of the project is reliability we setup persistent volume for the queue in which it will store the urls that it has already processed.  However this creates problem when you want to test the extractor as by running it consecutively it will no more process the urls if has already seen.  Thus the storage needs to be cleared before each run. This is done by running `$ docker volume rm rocnikovyprojekt_artemis-data`.

2. Producer - This is the aggregator part of the project. It will fetch the urls from the queue and process them.

3. Consumer - This is the processor part of the project. It will fetch the urls from the queue and extract them to folder output.  As we want the consumers to automatically close when the producers are done we set the `pills_to_die` to 1. This will make the consumer to die when it receives 1 pill from the queue.  Every queue produces exactly one pill when it has no more urls to process. This is why we set the `queue_size` to 1. This will make the queue to produce pill when it has no more urls to process.


Now we just need to run the `$ docker-compose up`.
We have also create shell script that will prune before running the docker-compose.
You can run it by `$ ./run.sh` (Probably the best idea).



.. note:: It's good idea to clear the output folder when testing extractor. The problem is that it is created by docker container so might need to raise up to admin priviilge to remove it.


.. note:: Files created at this tutorial can be found at :file:`examples/extractor_tutorial`

   



    








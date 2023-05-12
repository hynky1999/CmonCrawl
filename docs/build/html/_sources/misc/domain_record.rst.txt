.. _domain_record:

Domain Record
=============

By domain record we refer to a strucuture that cotains the information
about how to download a crawl of an url. It contains the following

* **url**: the url to crawl
* **filename**: the warc filename
* **offset**: the offset in the warc file
* **length**: the length of the html crawl
* **digest** [optional]: the digest of the html crawl
* **encoding** [optional]: the encoding of the html crawl
* **timestamp** [optional]: the timestamp of the crawl


.. _domain_record_jsonl:

Domain Record JSONL format
==========================

In order to use your own domain records with extract mode of cli,
you must format them into follwoing json format

.. code-block:: json

    {
        "domain_record": 
        {
            "url": "http://example.com",
            "filename": "crawl.warc.gz",
            "offset": 123,
            "length": 456,
            "digest: "sha1:1234567890abcdef",
            "encoding": "utf-8",
            "timestamp": "2018-01-01T00:00:00Z"
        },
        "additional_info":
        {
            "key1": "value1",
            "key2": "value2"
        }
    }

Each such json must be on a separate line in a file.
You don't have to provide all the fields, only ```url``, ```filename```,
```offset``` and ```length``` are required.
The Athena SQL keys are:
```u.url, cc.warc_filename, cc.warc_record_offset, cc.warc_record_length, cc.content_digest, cc.fetch_time```



The ```additional_info``` field is optional and can contain any additional
information. It will be added to extracted fields as is. It's usefull
when you for example want to add to which set the url belongs to.
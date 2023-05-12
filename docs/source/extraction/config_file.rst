.. _extractor_config:

Extractor config file
==========================

Structure
---------

In order to specify which extractor to use, you need to create a config
The structure is following:

.. code-block:: json

    {

        "extractors_path": "Path to the extractors folder",
        "routes": [
            {
                "regexes": [".*"],
                "extractors": [{
                    "name": "my_extractor",
                    "since": "iso date string",
                    "to": "iso date string"
                },
                {
                    "name": "my_extractor2",
                }
                ]
            },
            {
                "regexes": ["another_regex"],
                "....": "....
            }
        ]
    }


The ``extractors_path`` is the path to the folder where the extractors are located.

.. note::
    The extractors_path is relative to the current working directory.


The ``routes`` is a list of routes. Each route is a dictionary with the following keys:

* ``regexes``: a list of regexes. At least one regex must match the url, for this route to be used.
* ``extractors``: a list of extractors that will be used to extract the data from the url.


Each extractor has the following keys:

* ``name``: the name of the extractor. This is the name of the python file without the .py extension, you can also set NAME variable in the extractor file to override this.
* ``since`` [optional] : The starting crawl date for which the extractor is valid.  It must be full iso date string (e.g. 2009-01-01T00:00:00+00:00)
* ``to`` [optional] : The ending crawl date for which the extractor is valid.  Format is the same as for ``since``.

.. note::
    If ``since`` and ``to`` are not specified, the extractor will be used for all crawls.


Example
-------

Given the following folder structure:

.. code-block:: text

    extractors/
    ├── a_extractor.py
    ├── a_extractor2.py
    └── b_extractor.py

and the following config:

.. code-block:: json

    {

        "extractors_path": "./extractors",
        "routes": [
            {
                "regexes": [".*cmon.cz.*"],
                "extractors": [{
                    "name": "a_extractor",
                    "to": "2010-01-01T00:00:00+00:00"
                },
                {
                    "name": "a_extractor2",
                    "since": "2010-01-01T00:00:00+00:00"
                }
                ]
            },
            {
                "regexes": [".*cmon2.cz.*"],
                "extractors": [{
                    "name": "b_extractor",
                }
                ]
            }
        ]
    }

The following will happen:

* A domain record with url http://www.cmon.cz, cralwed on 2012 will be extracted using the a_extractor2.py extractor.
* A domain record with url http://www.cmon.cz, cralwed on 2009 will be extracted using the a_extractor.py extractor.
* A domain record with url http://www.cmon2.cz, cralwed on 2012 will be extracted using the b_extractor.py extractor.


`__init__.py`
-------------
You might want to put the common code of the extractors into
a common python file. The problem is that during the execution,
the extractors directory is not in the python path. To add the extractors
directory we also load `__init__.py`` file (But don't add load extractors in it).

Thus you can create `__init__.py` file in the extractors directory with the following content:

.. code-block:: python

    import sys
    from pathlib import Path
    sys.path.append(Path(__file__).parent)

which will add the extractors directory to the python path.


Arbitrary Code Execution
------------------------
.. warning::
    Since the router, loads and executes all files in the extractors
    directory, every .py file in this directory is executed. Thus
    you should not put any untrusted files in this directory.
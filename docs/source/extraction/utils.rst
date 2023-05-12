Extraction utils
================

The utilies for extraction are defined :py:mod:`cmoncrawl.processor.extraction`.
It provides helper function for both filtering and extraction.


Filtering
---------

- `must_exist_filter``: filter out the ulrs that don't contain css selector

- `must_not_exist_filter`: filter out the ulrs that contain css selector


Extraction
----------

-- `check_required`: Creates a function that checks if all the required fileds
    are present in the extracted data

-- `chain_transform`: Creates a function that chains multiple transformation function,
    if any return None, the chain is broken and None is returned.
    Especially usefull with soup select etc...

-- `extract_transform`: Creates a function that extracts the data from the soup
    tag using the css selector and transforms it using your transformation functions.
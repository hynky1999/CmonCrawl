![CmonCrawl Banner](./banner.webp)


## CommonCrawl Extractor with great versatility
![Build](https://github.com/hynky1999/CmonCrawl/actions/workflows/release.yml/badge.svg)
![Tests](https://github.com/hynky1999/CmonCrawl/actions/workflows/test_and_types.yml/badge.svg)
[![Documentation](https://github.com/hynky1999/CmonCrawl/actions/workflows/sphinx_build.yml/badge.svg)](https://hynky1999.github.io/CmonCrawl/)

![License](https://img.shields.io/badge/license-MIT-green.svg)
![Python Version](https://img.shields.io/badge/python-3.11-blue.svg)
[![PyPI](https://img.shields.io/badge/pypi-package-blue.svg)](https://pypi.org/project/cmoncrawl/)

Unlock the full potential of CommonCrawl data with `CmonCrawl`, the most versatile extractor that offers unparalleled modularity and ease of use.

## Why Choose CmonCrawl?

`CmonCrawl` stands out from the crowd with its unique features:

- **High Modularity**: Easily create custom extractors tailored to your specific needs.
- **Comprehensive Access**: Supports all CommonCrawl access methods, including AWS Athena and the CommonCrawl Index API for querying, and S3 and the CommonCrawl API for downloading.
- **Flexible Utility**: Accessible via a Command Line Interface (CLI) or as a Software Development Kit (SDK), catering to your preferred workflow.
- **Type Safety**: Built with type safety in mind, ensuring that your code is robust and reliable.

## Getting Started

### Installation

#### Install From PyPi
```bash
$ pip install cmoncrawl
```
#### Install From source
```bash
$ git clone https://github.com/hynky1999/CmonCrawl
$ cd CmonCrawl
$ pip install -r requirements.txt
$ pip install .
```

## Usage Guide

### Step 1: Extractor preparation
Begin by preparing your custom extractor. Obtain sample HTML files from the CommonCrawl dataset using the command:

```bash
$ cmon download --match_type=domain --limit=100 html_output example.com html
```
This will download a first 100 html files from *example.com* and save them in `html_output`.


### Step 2: Extractor creation
Create a new Python file for your extractor, such as `my_extractor.py`, and place it in the `extractors` directory. Implement your extraction logic as shown below:

```python
from bs4 import BeautifulSoup
from cmoncrawl.common.types import PipeMetadata
from cmoncrawl.processor.pipeline.extractor import BaseExtractor
class MyExtractor(BaseExtractor):
   def __init__(self):
      # you can force a specific encoding if you know it
      super().__init__(encoding=None)

   def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
      # here you can extract the data you want from the soup
      # and return a dict with the data you want to save
      body = soup.select_one("body")
      if body is None:
        return None
      return {
         "body": body.get_text()
      }

   # You can also override the following methods to drop the files you don't want to extracti
   # Return True to keep the file, False to drop it
   def filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
      return True
   def filter_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> bool:
      return True

# Make sure to instantiate your extractor into extractor variable
# The name must match so that the framework can find it
extractor = MyExtractor()
```

### Step 3: Config creation
Set up a configuration file, `config.json`, to specify the behavior of your extractor(s):
```json
{
    "extractors_path": "./extractors",
    "routes": [
        {
            # Define which url match the extractor, use regex
            "regexes": [".*"],
            "extractors": [{
                "name": "my_extractor",
                # You can use since and to choose the extractor based
                on the date of the crawl
                # You can ommit either of them
                "since": "2009-01-01",
                "to": "2025-01-01"
            }]
        },
        # More routes here
    ]
}
```

### Step: 4 Run the extractor
Test your extractor with the following command:

```bash
$ cmon extract config.json extracted_output html_output/*.html html
```

### Step 5: Full crawl and extraction
After testing, start the full crawl and extraction process:

#### 1. Retrieve a list of records to extract.

```bash
cmon download --match_type=domain --limit=100 dr_output example.com record
```

This will download the first 100 records from *example.com* and save them in `dr_output`. By default it saves 100_000 records per file, you can change this with the `--max_crawls_per_file` option.

#### 2. Process the records using your custom extractor.
```bash
$ cmon extract --n_proc=4 config.json extracted_output dr_output/*.jsonl record
```

Note that you can use the `--n_proc` option to specify the number of processes to use for the extraction. Multiprocessing is done on file level, so if you have just one file it will not be used.

## Advanced Usage

`CmonCrawl` was designed with flexibility in mind, allowing you to tailor the framework to your needs. For distributed extraction and more advanced scenarios, refer to our [documentation](https://hynky1999.github.io/CmonCrawl/) and the [CZE-NEC project](https://github.com/hynky1999/Czech-News-Classification-dataset).

## Examples and Support

For practical examples and further assistance, visit our [examples directory](https://github.com/hynky1999/CmonCrawl/tree/main/examples).

## Contribute

Join our community of contributors on [GitHub](https://github.com/hynky1999/CmonCrawl). Your contributions are welcome!

## License

`CmonCrawl` is open-source software licensed under the MIT license.

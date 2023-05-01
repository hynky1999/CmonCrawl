## CommonCrawl Extractor with great versatility


### Usage

#### Extractor preparation
You will want to start your custom extractor preparation.
To create them you need an example html files you want to extract.

You can use the following command to get html files from the CommonCrawl dataset:

```bash
$ cmondownload --limit=100 --output_type=html yoursite.com output_dir
```
This will download a first 100 html files from yoursite.com and save them in output_dir.

#### Extractor creation
Once you have your the files to extract, you can create your extractor.
To do so, you need to create a new python file e.g my_extractor.py in extractors directory and add the following code:

```python
from cmoncrawl.processor.pipeline.extractor import BaseExtractor
class MyExtractor(BaseExtractor):
   def __init__(self):
      # you can force a specific encoding if you know it
      super().__init__(encoding=None)

   def extract_soup(self, soup: BeautifulSoup, metadata: PipeMetadata):
      # here you can extract the data you want from the soup
      # and return a dict with the data you want to save

   # You can also override the following methods to drop the files you don't want to extracti
   # Return True to keep the file, False to drop it
   def filter_raw(self, response: str, metadata: PipeMetadata) -> bool:
      pass
   def filter_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> bool:
      pass

# Make sure to instantiate your extractor into extractor variable
# The name must match so that the framework can find it
extractor = MyExtractor()
```

### Config creation
Once you have your extractor, you need to create a config file to run the extractor.
In our case the config would look like this:

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
                "since": "2009-01-01T00:00:00+00:00",
                "to": "2009-01-01T00:00:00+00:00"
            }]
        },
        # More routes here
    ]
}
```

### Run the extractor
To test the extraction, you can use the following command:

```bash
$ cmonextract --mode=html html_file1 html_file2 ... html_fileN extraction_output_dir config_file
```

### Crawl the sites
Once you have your extractor tested, we can start crawling.
To do this you will proceed in two steps:

#### 1. Get the list of records to extract
To do this, you can use the following command:

```bash
$ cmondownload --limit=100000 --output_type=record yoursite.com output_dir
```

This will download the first 100000 records from yoursite.com and save them in output_dir. By default it saves 100_000 records per file, you can change this with the --max_crawl_per_file option.

#### 2. Extract the records
Once you have the records, you can use the following command to extract them:

```bash
$ cmonextract --nproc=4 --mode=record record_file1 record_file2 ... record_fileN extraction_output_dir config_file
```

Note that you can use the --nproc option to specify the number of processes to use for the extraction. Multiprocessing is done on file level, so if you have just one file it will not be used.


### Advanced usage
The whole project was written with modularity in mind. That means that you
can adjust the framework to your needs.

#TODO add more info about pipeline

Instead of first getting the records and then extracting them, you can do both in a distributed setting. For more info look at [CZE-NEC](https://github.com/hynky1999/Czech-News-Classification-dataset) project.

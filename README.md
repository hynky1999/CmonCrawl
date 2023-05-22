## CommonCrawl Extractor with great versatility

### Why is this solution better than others ?
Unlike all other commoncrawl extractors, this project allows creation of custom extractors with high level of modularity.
Unlike getting records from CmonCrawl index using Amazon's Athena this solution is completely free of cost :)


### Usage

#### Extractor preparation
You will want to start your custom extractor preparation.
To create them you need an example html files you want to extract.

You can use the following command to get html files from the CommonCrawl dataset:

```bash
$ cmon download --match_type=domain --limit=1000 example.com html_output html
```
This will download a first 100 html files from example.com and save them in html_output.

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
      return True
   def filter_soup(self, soup: BeautifulSoup, metadata: PipeMetadata) -> bool:
      return True

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
$ cmon extract config.json extracted_output html_output/*/*.html html
```

### Crawl the sites
Once you have your extractor tested, we can start crawling.
To do this you will proceed in two steps:

#### 1. Get the list of records to extract
To do this, you can use the following command:

```bash
cmon download --match_type=domain --limit=100000 example.com dr_output record
```

This will download the first 100000 records from example.com and save them in dr_output. By default it saves 100_000 records per file, you can change this with the --max_crawls_per_file option.

#### 2. Extract the records
Once you have the records, you can use the following command to extract them:

```bash
$ cmon extract --n_proc=4 config.json extracted_output dr_output/*/*.jsonl record
```

Note that you can use the --n_proc option to specify the number of processes to use for the extraction. Multiprocessing is done on file level, so if you have just one file it will not be used.


### Advanced usage
The whole project was written with modularity in mind. That means that you
can adjust the framework to your needs. To know more check  see [https://hynky1999.github.io/CmonCrawl/build/html/usage.html](documentation)

Instead of first getting the records and then extracting them, you can do both in a distributed setting. For more info look at [CZE-NEC](https://github.com/hynky1999/Czech-News-Classification-dataset) project.

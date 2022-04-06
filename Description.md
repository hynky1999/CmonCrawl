# Discussion on implementation

Layout problem with date

## Why CommonCrawl and not vanilla crawling ?

There are many reasons against vanilla crawling, this [article](https://scrapeops.io/blog/the-state-of-web-scraping-2022/) summarizes them pretty well.

### Main problems:

- Admins don't like bots making too many requests on their site -> various technics to block bots/crawlers from website eg. (ip filtering, canvas fingerprints). This needs to be dealt with.
- Cloudflare/Captcha protection on many sites
- Big bottleneck of network bandwidth (can be partially solved by events)
- Prevention of circles when discovering new urls
- Javascript rendering (SPA)

### Advantages:

- We can theoretically access any site available, with CC we can only retrieved urls indexed by CC

## Article url Discovery ?

In typical vanilla crawling, we build list of urls to visit as we go. Visit url, find links and add this links to frontier/list of urls to visit next. We also have to make sure we don't visit links multiple times to prevent cycling.

With CC things are much easier. Since space of all urls is finite, CC indexes all urls. We can than take such index and query for specified domain to get all urls with respect to that domain. Note that sometimes news servers use subdomain to distinguish between categories. This needs to be accounted for. Another important thing is that CC releases new crawls in monthly frequencies and for each crawl new index is created. Thus to retrieve urls from all crawls and remove duplicates.

As I said it is possible to query CC index, but there are multiple ways to do it.

1. [Query CDXJ Api provided by CC](https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html#api-reference) easier, but probably slower
2. [Manually Query CC Index Table](https://github.com/commoncrawl/cc-index-table/blob/master/README.md) harder, but faster

## Article Retrieve

Index value for given url, provides us with link for [WARC](https://www.iso.org/obp/ui/#!iso:std:68004:en) file containing the data of crawl of url. It also provides offset in WARC file so that there is no need to download whole WARC file.

## What is WARC file ?

In brief it is basically a data format for storing web crawls. For each url it contains HTTP response+request and content retrieved. Little bit more detailed info about [WARC](https://archive-it.org/blog/post/the-stack-warc-file/).

## How to Parse HTML file ?

We can of course do this manually, but I don't see any benefit in doing so.
Thus we are left with two libraries for parsing HTML files.

1. [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/bs4/doc)
2. [selectolax](https://selectolax.readthedocs.io/en/latest/lexbor.html)

From quick skimming through documentation, there is no significant difference. Both are capable parsing html document and then return nodes based on css selectors or path. I haven't found any reliable benchmark of these only few articles [1](https://medium.com/@ArtMyftiu/web-data-extraction-in-its-multitudes-using-python-b5849b92931c) , [2](https://rushter.com/blog/python-fast-html-parser/). Also people of HackerNews confirmed this bias so it must be true :)

## Concurrency

It is clear that program will be IO-bound. Thus we will decide between asyncio and threading. Since we will be spawning a lot of requests the fastest and best approach seems to be asyncio, because threading will use a lot of resources to orchestr threads.

## Cluster distribution

I plan to organize program to two parts. One will be responsible for getting urls to query from CC index and second for actual data retrieve. Thus we can retrieve urls for given domain and then divide them into N buckets and distribute each bucket to a node in cluster. This can be done simply by hashing url. Deployment using Docker could be considered.

## Error recovery and resuming

TBD

## Interesting Resources

https://michaelnielsen.org/ddi/how-to-crawl-a-quarter-billion-webpages-in-40-hours/ Although very old, it provides pretty good insights.
Redis idea seems to be pretty niche for resuming tasks and error recovery.

Deníček
mypy

Každé druhé pondělí v 11:00 v kanceláři N233.

# Short description

The goal of my semestr project is to create distributed web crawler
for extracting articles and metadata from the Czech news websites.
The crawler will not directly crawl the web but instead it will crawl archived websites
gathered by Common Crawl initiative.
For each website the program will retrieve all possible articles orignating on the website.
The articles will be cleared of all html tags and only the article text with its structure will be
extracted.
Aside from the article itself the program will retrieve these attributes if
present: [Headline, Brief/Abstract, Author, Publication date, Keywords,
Category, Count of comments in discussion section].

The program will support these websites.

### Supported websites:

- www.idnes.cz (2013-Today)
- www.novinky.cz (2013-2018) OR (2019-Today)
- www.aktualne.cz (2013-Today)
- www.seznamzpravy.cz (2018-Today)
- www.denik.cz (2013-Today)
- www.irozhlas.cz (2017-Today)
- www.ceskenoviny.cz (2016-Today)
- www.lidovky.cz (2013-Today)

The program will be written in Python 3.10 with usage of type annotations(typing).
Main focus will be given to scalability(deployment on cluster), fault-tolerance and high throughput.
The scalability factor will be achieved with usage of orchestration system Docker.
The fault-tolerance will be achived with usage of shared persisten storage eg. database,
namely speaking of Redis DB and RabitMQ message queues.
The last factor high single threaded throughput will be achieved with usage of asynchronous
programming using pythons asyncio library.

# Architecture

![helo](./Pipeline.drawio.svg)

The project will be splitted into two parts. Aggregator and Processor.
The main task of Aggregator is to aggregate all article urls for supported websites and pass them to Processor.
The main task of Processor is to extracted the data from websites

## Aggregator

In typical vanilla crawling, we build list of urls to visit as we go. Program visits the url, finds `href` links
and add these links to frontier/list of urls to visit next. Obviously there can be many links targeting one website,
thus another list of already visited websites is need in order to prevent duplicities and cycles in worst case.
With Common Crawl(CC) things are much easier. Since space of all urls is finite, CC maintains index
of urls it has crawled.
We can than query such index for specified domain to get all urls with respect
to that domain. Note that sometimes news servers use subdomain to distinguish between categories.
This needs to be accounted for. Another important thing is that CC releases new crawls in monthly
frequencies and for each crawl new index is created. Thus it is still needed to check for duplicate urls.
As I said it is possible to query CC index, but there are multiple ways to do it.

1. [Query CDXJ Api provided by CC](https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html#api-reference)
   Easy to query, speed depends on external API server
2. [Manually Query CC Index Table](https://github.com/commoncrawl/cc-index-table/blob/master/README.md)
   Big data framework needed for queries(Apache Hive/SparkSQL), but read should be faster

### Duplicates storage

As we wish to build a distributed application, we cannot use in memory lists, because we can run multiple
aggregators, thus data storage needs to be shared. For this task we will use RedisDB. It's in-memory key-value
database. RedisDB was chosen because we need fast R/W(this is achieved by storing data in-memory) and
we don't need complex data structures, thus key-value storage is fine. It also provides persistency with it's
memory snapshots. Another DB which was considered was Memcached. However it doesn't provide presitency and keys
can be only 256B longer, which could be an issue.

## Processor

The processor pipeline can be seen in overview image. We will describe its components now

### Downloader

This module will receive link to an WARC file and its offset and will download it and pass it to Router.

#### What is WARC file ?

In brief it is basically a data format for storing web crawls. For each url it contains HTTP response+request and content retrieved. Little bit more detailed info about [WARC](https://archive-it.org/blog/post/the-stack-warc-file/).

### Router

Router will lookup registred Extractors and choose an Extractor based on regex match in url.
Similiar idea to how [Django](https://www.djangoproject.com/) routes requests to views.

1000 souborů v jednom adresáři.

### Extractor

## How to Parse HTML file ?

We can of course do this manually, but I don't see any benefit in doing so.
Thus we are left with two libraries for parsing HTML files.

1. [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/bs4/doc)
2. [selectolax](https://selectolax.readthedocs.io/en/latest/lexbor.html)

From quick skimming through documentation, there is no significant difference. Both are capable parsing html document and then return nodes based on css selectors or path. I haven't found any reliable benchmark of these only few articles [1](https://medium.com/@ArtMyftiu/web-data-extraction-in-its-multitudes-using-python-b5849b92931c) , [2](https://rushter.com/blog/python-fast-html-parser/). Also people of HackerNews confirmed this bias so it must be true :)

## Concurrency

It is clear that program will be IO-bound. Thus we will decide between asyncio and threading. Since we will be spawning a lot of requests the fastest and best approach seems to be asyncio, because threading will use a lot of resources to orchestr threads.

## Cluster distribution

I plan to organize program to two parts. One will be responsible for getting urls to query from CC index and second for actual data retrieve. Thus we can retrieve urls for given domain and then divide them into N buckets and distribute each bucket to a node in cluster. This can be done simply by hashing url. Deployment using Docker could be considered.

## Error recovery and resuming

### Why CommonCrawl and not vanilla crawling ?

There are many reasons against vanilla crawling,
this [article](https://scrapeops.io/blog/the-state-of-web-scraping-2022/) summarizes them pretty well.

#### Main problems:

- Admins don't like bots making too many requests on their site -> various technics to block bots/crawlers from website eg. (ip filtering, canvas fingerprints). This needs to be dealt with.
- Cloudflare/Captcha protection on many sites
- Big bottleneck of network bandwidth (can be partially solved by events)
- Prevention of circles when discovering new urls
- Javascript rendering (SPA)

### Advantages:

- We can theoretically access any site available, with CC we can only retrieved urls indexed by CC

## Article url Discovery ?

In typical vanilla crawling, we build list of urls to visit as we go. Visit url, find links and add this links to frontier/list of urls to visit next. We also have to make sure we don't visit links multiple times to prevent cycling.

With CC things are much easier. Since space of all urls is finite, CC indexes all urls. We can than take such index and query for specified domain to get all urls with respect to that domain. Note that sometimes news servers use subdomain to distinguish between categories. This needs to be accounted for. Another important thing is that CC releases new crawls in monthly frequencies and for each crawl new index is created. Thus to retrieve urls from all crawls and remove duplicates.

As I said it is possible to query CC index, but there are multiple ways to do it.

1. [Query CDXJ Api provided by CC](https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html#api-reference) easier, but probably slower
2. [Manually Query CC Index Table](https://github.com/commoncrawl/cc-index-table/blob/master/README.md) harder, but faster

Formating using black
Pylance for types

Novinky cz, huge javascript leap
aktualne just cz->en + littble bit of structre change
seznamyzpravy same structure as new novinky.cz
denik.cz cz->en

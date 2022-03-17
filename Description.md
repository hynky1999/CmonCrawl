# Description of Project:

Create a program that will retrieve past articles(2013-2022) from supported websites.
The articles retrieved should be textual only, without any html tags (?? How about headings, should there be some specific handling ??). The structure of document should be kept even after removal of html tags eg.sections of text. Aside from article content other explicit metadata should be retrieved, namely: [Headline, Brief/Abstract, Author, Date of Creation, KeyWords??, Category??]. As a "database" for crawling program should use the open source project CommonCrawl. Program should support concurrency concepts (either raw threads or events). Program should retrieve data in one batch, continuous streaming will not have to be supported. Program should support Cluster deployment, thus program should employ existing orchestration system (Docker ???? k8s looks as overkill) or adhoc solution should be provided.

### Supported websites:
- www.idnes.cz
- www.novinky.cz
- www.aktualne.cz
- www.seznamzpravy.cz
- www.denik.cz
- www.irozhlas.cz
- www.ceskenoviny.cz
- www.lidovky.cz


CommonCrawl will be further addressed as CC
# Discussion on implementation

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

















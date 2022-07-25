# Main usage
Once all extractors are created and configured, you can start extracting.
## Docker
In order to use docker you need to have docker and docker-compose installed. If you want to deploy on cluster you will need to use swarm.

After any modification you make run `docker-compose build` to build the containers.
Then just run `docker-compose up` to start the extracting. All but artemis will shut down after extracting. In order to close artemis you need to run `docker-compose stop` or press Ctrl+C.

Since artemis is used for duplicates detection and since docker not always kills the containers properly make sure you run `docker-compose stop` before you run `docker-compose up`. By doing this you will reset duplicates detection which is good for debugging.

If plan to run on cluster make sure you bind volume to /var/lib/artemis/data in order to achieve persistency. Currently both messages and duplicates are preserved

Output will be placed to /var/output/{hostname}. Hostname needs to be there since streamers cannot really
work on same folder at the same time(will overflow defined limit), so streamers need to write to different folders. This is done in order to speed up the process since we don't have to check folder size every time before we try to write.
So if you want to see output bind it.

Artemis also provides dashboard which is accessible at http://localhost:8161/.
Also make sure you provide enough memory for artemis. YOU NEED TO DO THIS IN Artemis DOCKERFILE with -Xmx flag.

Sample configuration can be seen in docker-compose-sample.yml

## grid_run.sh
This is a script for deployment at cluster. The script is mad to work with open grid scheduler.

Similar setting applies to grid_run.sh as well.
Make sure you set up correct `PYTHON_PATH` and `ARTEMIS_HOST`.
Another very important setting is `FRESH_START`. By setting this to 1 you all artemis data will be deleted at execution. This will make sure you you start with fresh duplicate and message data. This is useful for debugging. But if you want to restart after crash make sure to set this to 0.

Output will be placed to ./output_{processor_num}
You can access the dashboard at ARTEMIS_HOST:6100.

If you wan to fiddle with disk speed you can change `ARTEMIS_RUN_PATH` and let it create instance somewhere with high speed disc.

No auto-scaling is implemented. If you want that use docker-swarm.


# Misc scripts
## download_article.py
This script is useful for download html files from Common crawl as reference for extracting.
You can specify limit of articles to download, time frame etc...

```
Example:
python download_article.py --limit=100 --since=2010-10-10 --to=2010-10-20 idnes.cz out
```

## Processor.process_article.py
This script is useful for testing extractors. Simply provide it with files you want to extract and it wil extract it.
It tries to deduce url from the html content. YOu can specify url manually. If you want to extract from multiple files you can use wildcard. Date can also be specified or script will try to deduce it from filename.

```
Example:
python process_article.py --url=http://www.idnes.cz/zpravy/slovensko/slovensko-je-v-krizi-s-evropou-a-je-to-v-krizi-s-evropo/ --date=2011-01-01 slovenkso1.html slovenkso2.html out
```
```

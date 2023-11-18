# import unittest
# from datetime import datetime
# from typing import List

# from cmoncrawl.aggregator.athena_query import (
#     DomainRecord,
#     MatchType,
# )
# from cmoncrawl.aggregator.index_query import IndexAggregator
# from cmoncrawl.aggregator.utils.helpers import get_all_CC_indexes, unify_url_id


# class TestIndexerAsync(unittest.IsolatedAsyncioTestCase):
#     async def asyncSetUp(self) -> None:
#         self.CC_SERVERS = ["https://index.commoncrawl.org/CC-MAIN-2022-05-index"]
#         self.di = await IndexAggregator(
#             ["idnes.cz"],
#             cc_servers=self.CC_SERVERS,
#             max_retry=100,
#             sleep_base=1.4,
#             prefetch_size=1,
#             match_type=MatchType.DOMAIN,
#         ).aopen()
#         self.client = self.di.client

#     async def asyncTearDown(self) -> None:
#         await self.di.aclose(None, None, None)

#     async def test_indexer_num_pages(self):
#         num_pages = await self.di.get_number_of_pages(
#             self.client,
#             self.CC_SERVERS[0],
#             "idnes.cz",
#             max_retry=20,
#             sleep_base=1.4,
#             match_type=MatchType.DOMAIN,
#         )
#         self.assertEqual(num_pages, 14)

#     async def test_indexer_all_CC(self):
#         indexes = await get_all_CC_indexes(self.client, self.di.cc_indexes_server)
#         indexes = sorted(indexes)
#         indexes = indexes[
#             : indexes.index("https://index.commoncrawl.org/CC-MAIN-2022-27-index") + 1
#         ]
#         self.assertEqual(len(indexes), 89)

#     async def test_since(self):
#         # That is crawl date not published date
#         self.di.since = datetime(2022, 1, 21)
#         self.di.limit = 5

#         async for record in self.di:
#             self.assertGreaterEqual(record.timestamp, self.di.since)

#     async def test_to(self):
#         # That is crawl date not published date
#         self.di.to = datetime(2022, 1, 21)
#         self.di.limit = 5

#         async for record in self.di:
#             self.assertLessEqual(record.timestamp, self.di.to)

#     async def test_limit(self):
#         records: List[DomainRecord] = []
#         self.di.limit = 10
#         async for record in self.di:
#             records.append(record)

#         self.assertEqual(len(records), 10)

#     async def test_init_queue_since_to(self):
#         iterator = self.di.IndexAggregatorIterator(
#             self.client,
#             self.di.domains,
#             [],
#             since=datetime(2022, 5, 1),
#             to=datetime(2022, 1, 10),
#             limit=None,
#             max_retry=10,
#             sleep_base=4,
#             prefetch_size=2,
#             match_type=MatchType.DOMAIN,
#         )
#         # Generates only for 2020
#         q = iterator.init_crawls_queue(
#             self.di.domains,
#             self.di.cc_servers
#             + [
#                 "https://index.commoncrawl.org/CC-MAIN-2021-43-index",
#                 "https://index.commoncrawl.org/CC-MAIN-2022-21-index",
#             ],
#         )
#         self.assertEqual(len(q), 2)

#     async def test_cancel_iterator_tasks(self):
#         self.di.limit = 10
#         async for _ in self.di:
#             pass
#         await self.di.aclose(None, None, None)
#         for iterator in self.di.iterators:
#             for task in iterator.prefetch_queue:
#                 self.assertTrue(task.cancelled() or task.done())

#     async def test_unify_urls_id(self):
#         urls = [
#             "https://www.idnes.cz/ekonomika/domaci/maso-polsko-drubezi-zavadne-salmonela.A190301_145636_ekonomika_svob",
#             "https://www.irozhlas.cz/ekonomika/ministerstvo-financi-oznami-lonsky-deficit-statniho-rozpoctu-_201201030127_mdvorakova",
#             "http://zpravy.idnes.cz/miliony-za-skodu-plzen-sly-tajemne-firme-do-karibiku-f9u-/domaci.aspx?c=A120131_221541_domaci_brm",
#             "http://zpravy.aktualne.cz/domaci/faltynek-necekane-prijel-za-valkovou-blizi-se-jeji-konec/r~ed7fae16abe111e4ba57002590604f2e/",
#             "https://video.aktualne.cz/dvtv/dvtv-zive-babis-je-pod-obrovskym-tlakem-protoze-nejsme-best/r~6c744d0c803f11eb9f15ac1f6b220ee8/",
#             "https://zpravy.aktualne.cz/snih-komplikuje-dopravu-v-praze-problemy-hlasi-i-severni-a-z/r~725593e0279311e991e8ac1f6b220ee8/",
#             "https://www.seznamzpravy.cz/clanek/domaci-zivot-v-cesku-manazer-obvineny-s-hlubuckem-za-korupci-ma-dostat-odmenu-az-13-milionu-209379",
#             "https://www.denik.cz/staty-mimo-eu/rusko-ukrajina-valka-boje-20220306.html",
#             "http://www.denik.cz/z_domova/zdenek-skromach-chci-na-hrad-ale-proti-zemanovi-nepujdu-20150204.html",
#             "https://www.denik.cz/ekonomika/skoda-auto-odbory-odmitly-navrh-firmy-20180209.html",
#             "http://data.blog.ihned.cz/c1-59259950-data-retention-zivot-v-zaznamech-mobilniho-operatora",
#             "http://archiv.ihned.cz/c1-65144800-south-stream-prijde-gazprom-draho-firma-pozaduje-za-zruseny-projekty-stovky-milionu-euro",
#             "http://www.novinky.cz/domaci/290965-nove-zvoleneho-prezidenta-si-hned-prevezme-ochranka.html",
#             "https://www.novinky.cz/zahranicni/svet/clanek/nas-vztah-s-ruskem-zapad-spatne-pochopil-rika-cina-40403627",
#             "https://www.novinky.cz",
#             "https://pocasi.idnes.cz/?t=img_v&regionId=6&d=03.12.2019%2005:00&strana=3",
#             "https://idnes.cz/ahoj@1",
#         ]
#         urls_ids = [
#             "idnes.cz/ekonomika/domaci/maso-polsko-drubezi-zavadne-salmonela",
#             "irozhlas.cz/ekonomika/ministerstvo-financi-oznami-lonsky-deficit-statniho-rozpoctu",
#             "zpravy.idnes.cz/miliony-za-skodu-plzen-sly-tajemne-firme-do-karibiku-f9u-/domaci",
#             "zpravy.aktualne.cz/domaci/faltynek-necekane-prijel-za-valkovou-blizi-se-jeji-konec/r",
#             "video.aktualne.cz/dvtv/dvtv-zive-babis-je-pod-obrovskym-tlakem-protoze-nejsme-best/r",
#             "zpravy.aktualne.cz/snih-komplikuje-dopravu-v-praze-problemy-hlasi-i-severni-a-z/r",
#             "seznamzpravy.cz/clanek/domaci-zivot-v-cesku-manazer-obvineny-s-hlubuckem-za-korupci-ma-dostat-odmenu-az-13-milionu",
#             "denik.cz/staty-mimo-eu/rusko-ukrajina-valka-boje",
#             "denik.cz/z_domova/zdenek-skromach-chci-na-hrad-ale-proti-zemanovi-nepujdu",
#             "denik.cz/ekonomika/skoda-auto-odbory-odmitly-navrh-firmy",
#             "data.blog.ihned.cz/c1-59259950-data-retention-zivot-v-zaznamech-mobilniho-operatora",
#             "archiv.ihned.cz/c1-65144800-south-stream-prijde-gazprom-draho-firma-pozaduje-za-zruseny-projekty-stovky-milionu-euro",
#             "novinky.cz/domaci/290965-nove-zvoleneho-prezidenta-si-hned-prevezme-ochranka",
#             "novinky.cz/zahranicni/svet/clanek/nas-vztah-s-ruskem-zapad-spatne-pochopil-rika-cina",
#             "novinky.cz",
#             "pocasi.idnes.cz",
#             "idnes.cz/ahoj",
#         ]
#         for i, url in enumerate(urls):
#             self.assertEqual(unify_url_id(url), urls_ids[i])


# if __name__ == "__main__":
#     unittest.main()

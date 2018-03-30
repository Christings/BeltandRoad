# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from scrapy.conf import settings
from .items import BeltandRoadItem
import csv


# 一带一路战略支撑平台
class BeltandRoadPipeline(object):
    def __init__(self, mongo_uri, mongo_db, mongo_port):
        self.mongo_uri = mongo_uri
        self.mongo_port = mongo_port
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'),
                   mongo_port=crawler.settings.get('MONGO_PORT'),
                   mongo_db=crawler.settings.get('MONGO_DB')
                   )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri, self.mongo_port)
        self.db = self.client[self.mongo_db]
        self.BeltandRoad = self.db['BeltandRoad']

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if isinstance(item, BeltandRoadItem):
            try:
                if item['name']:
                    item = dict(item)
                    # self.db[item.collection].insert(item)  运行这个代码时利用items中collection创建表，会提示插入失败，但是依然会插入到数据库？
                    self.BeltandRoad.insert(item)
                    print("插入成功")
                    return item
            except Exception as e:
                spider.logger.exception("插入失败")

                # def __init__(self):
                #     with open('BeltandRoad.csv', 'w') as csvout:
                #         self.csvwriter = csv.writer(csvout)
                #         self.csvwriter.writerow([b'name', b'content'])
                #
                # def process_item(self, item, spider):
                #     if isinstance(item, BeltandRoadItem):
                #         try:
                #             rows = zip(item['name'], item['content'])
                #             for row in rows:
                #                 self.csvwriter.writerow(row)
                #             self.csvwriter.close()
                #             return item
                #
                #             # self.ws.append(line)
                #             # self.wb.save('/CaasSpider/files/BeltandRoad.xlsx')
                #         except Exception as e:
                #             spider.logger.exception("")

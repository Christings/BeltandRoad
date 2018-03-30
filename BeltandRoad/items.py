# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


# 一带一路战略支撑平台
class BeltandRoadItem(scrapy.Item):
    # collection = 'BeltandRoad'

    name = scrapy.Field()  # 机构名称
    intro = scrapy.Field()  # 机构简介
    address = scrapy.Field()  # 机构地址
    tel = scrapy.Field()  # 机构电话
    fax = scrapy.Field()  # 机构传真
    email = scrapy.Field()  # 机构邮箱
    site = scrapy.Field()  # 机构网址

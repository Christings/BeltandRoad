>本文首发于我的博客：[http://gongyanli.com](http://gongyanli.com/%E5%88%A9%E7%94%A8Scrapy%E6%8A%93%E5%8F%96%E4%B8%80%E5%B8%A6%E4%B8%80%E8%B7%AF%E6%88%98%E7%95%A5%E6%80%A7%E6%94%AF%E6%92%91%E5%B9%B3%E5%8F%B0%E4%BF%A1%E6%81%AF/)  
>代码传送门：[https://github.com/Gladysgong/BeltandRoad](https://github.com/Gladysgong/BeltandRoad)  
>简书: [https://www.jianshu.com/p/6fe4afa1b98a](https://www.jianshu.com/p/6fe4afa1b98a)  
>CSDN: [https://blog.csdn.net/u012052168/article/details/79761833](https://blog.csdn.net/u012052168/article/details/79761833)

	好久以前做的东西了，网站的数据也很容易拿到，最近想对自己的东西做一个总结，所以就有了这篇文章。	
	我的目标是抓取一带一路战略支撑平台里面一些机构的数据，像联系电话、邮箱等等，简单看看网站的样子把。
	首先获取列表项的所有url，其中包括一项翻页操作，拿到url后访问，获取里面的详细信息，就这么简单。
![](http://p2lakvkq0.bkt.clouddn.com/beltandroad.jpg)
## 一、定义Items
	`class BeltandRoadItem(scrapy.Item):
    # collection = 'BeltandRoad'

    name = scrapy.Field()  # 机构名称
    intro = scrapy.Field()  # 机构简介
    address = scrapy.Field()  # 机构地址
    tel = scrapy.Field()  # 机构电话
    fax = scrapy.Field()  # 机构传真
    email = scrapy.Field()  # 机构邮箱
    site = scrapy.Field()  # 机构网址`
## 二、爬虫模块
	爬虫模块中包括翻页操作，并不复杂，就不详细说明。

----------

	`# -*- coding:utf-8 -*-
	from scrapy.spiders import CrawlSpider, Rule
	import scrapy
	from ..items import BeltandRoadItem
	
	# from scrapy.conf import settings
	
	# 一带一路战略支撑平台
	class BeltandRoadSpider(CrawlSpider):
	    name = "BeltandRoadSpider"
	    start_urls = ['http://ydyl.drcnet.com.cn/www/ydyl/channel.aspx?version=YDYL&uid=8011']
	
	    # 解析url地址
	    def parse(self, response):
	        # urls = response.xpath('//div[@class="pub_right"]/ul/li/div[1]/a/@href').extract()
	        urls = response.xpath('//ul[@id="ContentPlaceHolder1_WebPageDocumentsByUId1"]/li/div[1]/a/@href').extract()
	        # institute_name = response.xpath('//ul[@class="left-nav"]/li/h3/a/text()').extract()
	
	        for url in urls:
	            # url = "http://ydyl.drcnet.com.cn/www/ydyl/" + url
	            # print("1:", url)
	            yield scrapy.Request(url=url, callback=self.parse_content)
	
	        next_page = response.xpath(
	            '//div[@id="ContentPlaceHolder1_WebPageDocumentsByUId1_PageRow"]/input[4]/@onclick').extract()
	        next_page = str(next_page).split("\'")[1]
	        print("next:", next_page)
	        if next_page:
	            next_page = "http://ydyl.drcnet.com.cn" + next_page
	            yield scrapy.Request(url=next_page, callback=self.parse)
	
	    # 解析内容
	    def parse_content(self, response):
	        item = BeltandRoadItem()
	        name = response.xpath('//div[@id="disArea"]/strong/div/text()').extract()[0]  # 提取名称
	        content = response.xpath('//div[@id="disArea"]/div[@id="docContent"]/p').xpath('string(.)').extract()  # 提取其他信息
	        content = str(content).split('\'')
	        item['name'] = name
	        for i in range(len(content)):  # 过滤无效信息后，提取有用信息存储到item中
	            if content[i] != '[' and content[i] != ']' and content[i] != ', ':
	                print("xx:", content[i])
	                if "简介" in content[i]:
	                    item['intro'] = content[i]
	                elif "地址" in content[i]:
	                    item['address'] = content[i]
	                elif "联系电话" in content[i]:
	                    item['tel'] = content[i]
	                elif "传真" in content[i]:
	                    item['fax'] = content[i]
	                elif "电子邮箱" in content[i]:
	                    item['email'] = content[i]
	                elif "网址" in content[i]:
	                    item['site'] = content[i]
	        yield item
	`
## 三、构建pipelines
	主要就是进行数据持久化操作，把数据存入MongoDB数据中。

----------

    `import pymongo
	from scrapy.conf import settings
	from .items import BeltandRoadItem
	
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
	                spider.logger.exception("插入失败")`
## 四、配置文件settings
	
	# 激活pipelines
	ITEM_PIPELINES = {
	   'BeltandRoad.pipelines.BeltandRoadPipeline': 300,}

	# 数据库配置
	MONGO_URI = "127.0.0.1"  # 主机IP
	MONGO_PORT = 27017  # 端口号
	MONGO_DB = "Belt"  # 数据库名字
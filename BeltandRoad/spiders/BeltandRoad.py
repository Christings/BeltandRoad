# -*- coding:utf-8 -*-
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

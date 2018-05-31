# -*- coding: utf-8 -*-
import scrapy
import datetime

from scrape_n_geo.items import CompletedSalesItem
from scrapy.loader import ItemLoader


class JscSpider(scrapy.Spider):
    name = 'jsc'
    allowed_domains = []
    start_urls = ['http://www.tjsc.com/Sales/CompletedSales/']

    def parse(self, response):
        sales = response.xpath('//*[@id="basic-datatables"]/tbody//tr')
        path = '//*[@id="basic-datatables"]/tbody//tr'
        # import pdb; pdb.set_trace()
        loader = ItemLoader(item=CompletedSalesItem(), response=response)
        for sale in sales:
            item = CompletedSalesItem()

            item['sale_date'] = sale.xpath( 'td[1]/text()').extract_first().strip()
            item['sale_time'] = sale.xpath( 'td[2]/text()').extract_first().strip()
            item['file_number'] = sale.xpath( 'td[3]/text()').extract_first().strip()
            item['case_number'] = sale.xpath( 'td[4]/text()').extract_first().strip()
            item['firm_name'] = sale.xpath( 'td[5]/text()').extract_first().strip()
            item['address'] = sale.xpath( 'normalize-space(td[6]//a/text())').extract_first().strip()
            item['city'] = sale.xpath( 'td[7]/text()').extract_first().strip()
            item['county'] = sale.xpath( 'td[8]/text()').extract_first().strip()
            item['zip_code'] = sale.xpath( 'td[9]/text()').extract_first().strip()
            item['opening_bid'] = sale.xpath( 'td[10]/text()').extract_first().strip()
            item['required_down'] = sale.xpath( 'td[11]/text()').extract_first().strip()
            item['sale_amount'] = sale.xpath( 'td[12]/text()').extract_first().strip()
            item['continuance'] = sale.xpath( 'td[13]/text()').extract_first().strip()
            item['sold_to'] = sale.xpath( 'td[14]/text()').extract_first().strip()
            item['last_updated'] = datetime.datetime.now()
            yield item




# table_rows = response.xpath('//*[@id="basic-datatables"]/tbody//tr')
# data = {}
# for table_row in table_rows:
#     data[table_row.xpath('td[/text()').extract_first().strip()] = table_row.xpath('td[@class="col2 strong"]/text()').extract_first().strip()
# yield data

# def parse(self, response):
#     products = response.xpath('//*[@id="Year1"]/table//tr')
#     # ignore the table header row
#     for product in products[1:]:
#         item = Schooldates1Item()
#         item['hol'] = product.xpath('td[1]//text()').extract_first()
#         item['first'] = product.xpath('td[2]//text()').extract_first()
#         item['last'] = ''.join(product.xpath(
#             'td[3]//text()').extract()).strip()
#         item['url'] = response.url
#         yield item

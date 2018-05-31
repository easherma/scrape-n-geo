# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import datetime
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join


class CompletedSalesItem(scrapy.Item):

    sale_date = scrapy.Field()
    sale_time = scrapy.Field()
    file_number = scrapy.Field()
    case_number = scrapy.Field()
    firm_name = scrapy.Field()
    address = scrapy.Field()
    address_components = scrapy.Field()
    geocode_result = scrapy.Field(serializer=str)
    geocoded_address = scrapy.Field()
    # lat = scrapy.Field()
    # lng = scrapy.Field()
    geocode_url = scrapy.Field()
    city = scrapy.Field()
    county = scrapy.Field()
    zip_code = scrapy.Field()
    opening_bid = scrapy.Field()
    required_down = scrapy.Field()
    sale_amount = scrapy.Field()
    continuance = scrapy.Field()
    sold_to = scrapy.Field()
    zoning_query = scrapy.Field()
    county_query = scrapy.Field()
    last_updated = scrapy.Field(serializer=str)

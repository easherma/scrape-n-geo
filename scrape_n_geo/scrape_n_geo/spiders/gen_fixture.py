import scrapy
from datetime import datetime
import time


class FixtureSpider(scrapy.Spider):
    name = "gen_fixture"

    start_urls = [
        "http://www.tjsc.com/Sales/CompletedSales/",
    ]

    def parse(self, response):
        timestamp = datetime.now().isoformat(timespec='minutes')
        timestamp = datetime.now().strftime('%m%d%Y_%H%M')
        filename = '{}_{}.html'.format(self.name, timestamp)
        with open(filename, 'wb') as f:
            f.write(response.body)

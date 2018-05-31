# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
from scrapy.exporters import JsonItemExporter, CsvItemExporter
from scrapy.exceptions import DropItem
from datetime import datetime
import re
import geocoder
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time


def generate_file_name(file_format):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    file_name = './local_outputs/{}_{}.{}'.format(
        'output', timestamp, file_format)
    return file_name


def parse_date(date):
    datemask = "%m/%d/%Y"
    datetime_object = datetime.strptime(date, datemask)
    return datetime_object


def datetime_parse(date, time):
    return datetime.strptime("{}, {}".format(date, time), "%m/%d/%Y, %H:%M %p", )


def strip_non_digits(column):
    cast = re.sub('[^.0-9]', '', column)
    return cast


class DefaultValuesPipeline(object):

    def process_item(self, item, spider):
        if item:
            for field in item.fields:
                item.setdefault(field, '')

        return item


class FilterPipeline(object):
    city = "CHICAGO"
    sale_threshold = "$0.00"
    include_continuance = False

    def process_item(self, item, spider):
        if item['city']:
            if item['city'] == "CHICAGO" and item['sale_amount'] != "$0.00" and item['continuance'] == "":
                return item
            else:
                print("city: ", item['city'], item['city'] == "CHICAGO")
                print("sale amount: ",
                      item['sale_amount'], item['sale_amount'] != "$0.00")
                print("continuance", item['continuance'],
                      item['continuance'] == "")
                raise DropItem(
                    "Filtering element since it dosen't match criteria")
        else:
            print('some error')


class cleanPipeline(object):
    city = "CHICAGO"
    sale_threshold = "$0.00"

    def replace_chars(text):
        """
        replace 'bad' chars that throw off geocoding
        """
        for ch in ['\.', '\#', '\/', '\;']:
            if ch in text:
                text = text.replace(ch, '' + ch)

    def truncate_values(item, length):
        return dict((k, v[:length]) for k, v in item.items())

    # def truncate_values(item, length):
    #     return (k, v[:length])

    def process_item(self, item, spider):
        try:
            if item:
                # truncate long address strings(should clean)
                print(len(item['address']))
                if len(item['address']) > 80:
                    item['address'] = item['address'][:80]
                item['sale_amount'] = strip_non_digits(item['sale_amount'])
                item['opening_bid'] = strip_non_digits(item['opening_bid'])
                item['sale_date'] = datetime_parse(
                    item['sale_date'], item['sale_time'])
                return item
            else:
                pass
        except Exception as e:
            print("EXCEPTION:", e)
            pass


class AddressPipeline(object):
    """
    Process an item.
    """

    def process_item(self, item, spider):
        # import pdb; pdb.set_trace()
        address_components = [item.get(x)
                              for x in ['address', 'city', 'zip_code']]
        item['address_components'] = ",".join(
            str(x) for x in address_components)
        item['address_components'] = item['address_components'] + ",IL"
        return item


class GeocoderPipeline(object):

    def __init__(self):
        # the geocoder library. this way we can use other libraries if needed or write/extend our own
        self.engine = 'geocoder'
        # api key, mapbox in this case. best practice would be to hide this

    def requests_retry_session(
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
    ):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def process_item(self, item, spider):

        try:
            geocode_result = self.get_geocoder_query(
                item['address_components'], provider='mapbox' )
            if geocode_result.quality <= 0.7:
                print("inital quality too low: ",
                      geocode_result.quality, "switching provider")
                geocode_result = self.get_geocoder_query(
                    item['address_components'],  provider='google')
            item["geocode_result"] = geocode_result.json
            item["geocoded_address"] = geocode_result.address
            item['geocode_url'] = geocode_result.url
            print("geocoded: ", item['geocode_url'])
            return item
        except Exception as e:
            print("quality error", e)
            raise


    def get_geocoder_query(self, address=None, bbox=None, key=None, provider=None, *args, **kwargs):

        try:
            if provider == 'mapbox':
                key = 'pk.eyJ1IjoiZWFzaGVybWEiLCJhIjoiY2oxcW51Nzk2MDBkbTJxcGUxdm85bW5xayJ9.7mL0wQ7cjifWwt5DrXMuJA'  # API
            if provider == 'google':
                key = 'AIzaSyBi7dtBuJ0sCzUEz8dhIRnOteUVeKWkCvE'
        except:
            print("no query provider")

        params = {
            'engine': self.engine,
            'provider': provider,
            'key': key

        }
        query_params = {

            'address': address,
            'bbox': bbox,
            'key': key
        }
        if key:
            with self.requests_retry_session() as session:
                t0 = time.time()

                try:
                    #                     response = mb_geo.get_geocoder_query(address_strings[428], session=session)
                    geocode_result = getattr(geocoder, params['provider'])(
                        query_params['address'], bbox=query_params['bbox'], key=key, session=session)
                    return geocode_result
                except Exception as x:
                    print('It failed :(', x.__class__.__name__)
                else:
                    print('It eventually worked', geocode_result.status)
                    return geocode_result
                finally:
                    t1 = time.time()
                    print('Took', t1 - t0, 'seconds')
#             geocode_result = getattr(geocoder, params['provider'])(*args, self.key)
#             geocode_result = getattr(geocoder, params['provider'])(query_params['address'], bbox = query_params['bbox'], key = self.key)
        else:
            print("no key")
            geocode_result = getattr(geocoder, params['provider'])(
                query_params['address'], proximity=query_params['bbox'])

        print(geocode_result.status)
        if geocode_result.status != 'OK':
            print("Error geocoding {}: {}".format(
                address, geocode_result.status))
#             geocode_result = "error"
            return geocode_result

        return geocode_result


class JsonWriterPipeline(object):

    def __init__(self):
        self.file = open(generate_file_name('json'), 'wb')
        self.exporter = JsonItemExporter(
            self.file, encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class CsvWriterPipeline(object):

    def __init__(self):
        self.file = open(generate_file_name('csv'), 'wb')
        self.exporter = CsvItemExporter(self.file, encoding='utf-8')
        self.exporter.fields_to_export = [
                    'sale_date',
                    'sale_time',
                    'file_number',
                    'case_number',
                    'opening_bid',
                    'required_down',
                    'sale_amount',
                    'continuance',
                    'sold_to',
                    'firm_name',
                    'address',
                    'city',
                    'county',
                    'zip_code',
                    'address_components',
                    # 'lat',
                    # 'lng',
                    'last_updated',
                    'geocode_result',
                    'geocoded_address'
                    'geocode_url'
                    ]
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

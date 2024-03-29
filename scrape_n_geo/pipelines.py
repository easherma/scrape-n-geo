# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import pandas as pd
import csv
from scrapy.exporters import JsonItemExporter, CsvItemExporter
from scrapy.exceptions import DropItem
from datetime import datetime
import re
import geocoder
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
import logging
import os
import pathlib
import traceback


def generate_file_name(file_format, custom_name):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    file_name = './local_outputs/{}_{}.{}'.format(
        custom_name, timestamp, file_format)
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


def replace_chars(text):
    bad_chars = ['.', '#', '/', ';']
    clean = re.sub('[|.|#|/|;]', '', text)
    return clean


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
                raise DropItem(
                    "Filtering element since it dosen't match criteria")
        else:
            print('some error')


class cleanPipeline(object):
    city = "CHICAGO"
    sale_threshold = "$0.00"

    def truncate_values(item, length):
        return dict((k, v[:length]) for k, v in item.items())

    def process_item(self, item, spider):
        try:
            if item:
                # truncate long address strings(should clean)
                if len(item['address']) > 80:
                    item['address'] = item['address'][:80]
                item['address'] = replace_chars(item['address'])
                item['sale_amount'] = strip_non_digits(item['sale_amount'])
                item['opening_bid'] = strip_non_digits(item['opening_bid'])
                item['sale_date'] = datetime_parse(
                    item['sale_date'], item['sale_time'])
                return item
            else:
                pass
        except Exception as e:
            logging.error("EXCEPTION:", e)
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

        # print(geocode_result.status)
        if geocode_result.status != 'OK':
            logging.error("Error geocoding {}: {}".format(
                address, geocode_result.status))
#             geocode_result = "error"
            return geocode_result

        return geocode_result

    def process_item(self, item, spider):

        try:
            geocode_result = self.get_geocoder_query(
                item['address_components'], provider='mapbox')
            if geocode_result:
                if geocode_result.quality <= 0.7:
                    logging.info("inital quality too low: ",
                                 geocode_result.quality, "switching provider")
                    geocode_result = self.get_geocoder_query(
                        item['address_components'],  provider='google')
            else:
                logging.info("no inital result: ",
                             geocode_result.quality, "switching provider")
                geocode_result = self.get_geocoder_query(
                    item['address_components'],  provider='google')

            item["geocode_result"] = geocode_result
            item["geocoded_latlng"] = geocode_result.latlng
            item["geocoded_address"] = geocode_result.address
            item['geocode_url'] = geocode_result.url

            logging.info(
                "geocoded: ", item['geocode_url'], "quality: ",  geocode_result.quality)
            return item
        except Exception as e:
            logging.error("quality error", e)
            raise


class AttributesPipeline(object):

    # make api links
    def create_api_links(self, latlng):
        zoning_endpoint = "https://data.cityofchicago.org/resource/dj47-wfun.geojson?$where=intersects(the_geom,'"
        county_endpoint = "https://gis12.cookcountyil.gov/arcgis/rest/services/cookVwrDynmc/MapServer/44/query?where=&text=&objectIds=&time=&geometry=POINT("
        county_params = "&geometryType=esriGeometryPoint&inSR=4326&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=BLDGClass,Pin14,TotalValue&returnGeometry=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&f=pjson"
        zoning_query = ""
        county_query = ""
        try:
            zoning_query = zoning_endpoint + latlng.wkt + "')"
            county_query = county_endpoint + \
                str(latlng.lng) + "," + str(latlng.lat) + ")" + county_params

        except:
            logging.error("link creation error")
            pass
        finally:
            return zoning_query, county_query

    # get county
    def get_county(self, url):
        BLDGClass = None
        PIN14 = None
        TotalValue = None
        attributes = None
        try:
            r = requests.get(url)
            result = r.json()
            BLDGClass = result['features'][0]['attributes']['BLDGClass']
            PIN14 = result['features'][0]['attributes']['PIN14']
            TotalValue = result['features'][0]['attributes']['TotalValue']
            attributes = result['features'][0]['attributes']
        except Exception as e:
            logging.error(e)
            attributes = None
            pass

        return attributes, BLDGClass

    def get_zoning(self, url):
        # make request using parsed url w/ params
        zone_class = None
        try:
            r = requests.get(url)
            json = r.json()
            zone_class = json['features'][0]['properties']['zone_class']
        except Exception as e:
            logging.error(e)
            zone_class = None
            pass

        return zone_class

    def estimate_units(self, code):

        try:
            code = int(code)
        except:
            pass

        #
        # import pdb; pdb.set_trace()

        lookup_dict = {'BLDGClass': {0: 200, 1: 202, 2: 203, 3: 204, 4: 205, 5: 206, 6: 207, 7: 208, 8: 209, 9: 210, 10: 211, 11: 212, 12: 234, 13: 278, 14: 299, 15: 313, 16: 314, 17: 315, 18: 318, 19: 391, 20: 396, 21: 399, 22: 913, 23: 914, 24: 915, 25: 918, 26: 959, 27: 991, 28: 996, 29: 997}, 'min_units': {0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1, 8: 1,
                                                                                                                                                                                                                                                                                                                        9: 1, 10: 2, 11: 1, 12: 1, 13: 1, 14: 1, 15: 7, 16: 1, 17: 1, 18: 7, 19: 7, 20: 7, 21: 1, 22: 7, 23: 1, 24: 1, 25: 1, 26: 1, 27: 1, 28: 7, 29: 1}, 'max_units': {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0, 10: 6, 11: 6, 12: 0, 13: 0, 14: 0, 15: 0, 16: 0, 17: 0, 18: 0, 19: 0, 20: 0, 21: 0, 22: 0, 23: 0, 24: 0, 25: 0, 26: 0, 27: 0, 28: 0, 29: 0}}

        # with open('scrape_n_geo/lookup.py', 'r') as f:
        #     s = f.read()
        #     units_lookup = pd.DataFrame.from_dict(eval(s))
        units_lookup = pd.DataFrame.from_dict(lookup_dict)
        if code in units_lookup['BLDGClass'].tolist():
            try:
                building_code = units_lookup[units_lookup['BLDGClass'] == code]
                u_min = int(building_code['min_units'])
                u_max = int(building_code['max_units'])
                if (u_min < u_max):
                    return u_max
                else:
                    return u_min
            except Exception as e:
                logging.error("no code", e)
                traceback.print_exc()
                return 0
        else:
            return 0

    def process_item(self, item, spider):
        # import pdb; pdb.set_trace()
        if item:
            try:
                api_links = self.create_api_links(item["geocode_result"])
                zoning_values = self.get_zoning(api_links[0])
            except Exception as e:
                logging.error.exception(e)
                logging.error(
                    "Error getting zoning attributes with {}".format(item["geocode_result"]))
            try:
                county_values = self.get_county(api_links[1])
            except Exception as e:
                logging.error(e)
                logging.error(
                    "Error getting county attributes with {}".format(item["geocode_result"]))
            if zoning_values:
                item["zoning"] = zoning_values

            if county_values:
                item["county_attributes"] = county_values[0]
                item["county_class"] = county_values[1]
                item['estimated_units'] = str(
                    self.estimate_units(county_values[1]))

            item['zoning_query'] = api_links[0]
            item['county_query'] = api_links[1]
            item["geocode_result"] = item["geocode_result"].json
            # import pdb; pdb.set_trace()
            item["street_address_1"] = f"{item['geocode_result']['housenumber'] if 'housenumber' in item['geocode_result'].keys() else ''} {item['geocode_result']['raw']['text']}"
            item["city"] = item['geocode_result']['city']
            item["state"] = item['geocode_result']['raw']['region']

            return item
        else:

            raise


class JsonWriterPipeline(object):

    def __init__(self):
        self.file = open(generate_file_name('json', 'output'), 'wb')
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
        self.file = open(generate_file_name('csv', 'output'), 'wb')
        self.exporter = CsvItemExporter(self.file, encoding='utf-8')

        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class PrinterWriterPipeline(object):
    """
    filtered csv file for easy reading
    """

    def __init__(self):
        self.file = open(generate_file_name('csv', 'for_printer'), 'wb+')
        self.exporter = CsvItemExporter(self.file, encoding='utf-8')
        self.exporter.fields_to_export = [
            'street_address_1',
            'city',
            'state',
            'zip_code',
            'case_number',
            'zoning',
            'county_class',
            'estimated_units',
            'address'
        ]
        # import pdb; pdb.set_trace()
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

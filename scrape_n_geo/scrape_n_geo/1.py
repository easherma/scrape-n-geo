class MapboxPipeline(object):

    def __init__(self, p):
        # the geocoder library. this way we can use other libraries if needed or write/extend our own
        self.engine = 'geocoder'
        # api key, mapbox in this case. best practice would be to hide this
        self.key = 'pk.eyJ1IjoiZWFzaGVybWEiLCJhIjoiY2oxcW51Nzk2MDBkbTJxcGUxdm85bW5xayJ9.7mL0wQ7cjifWwt5DrXMuJA'  # API

    def process_item(self, item, spider):
        try:
            # #import pdb; pdb.set_trace()
            print("geocoding  ", item)
            with requests.Session() as session:
                response = geocoder.mapbox(
                    item['address_components'],
                    bbox=[-87.940102, 41.643921, -87.523987, 42.023022], session=session, key=self.key)
                #I'm not sure the above session is being persisted the way I would like
                item['lng'] = response[0].lng
                item['lat'] = response[0].lat

                item['geocode_url'] = response.url
                print("geocoded: ", item['geocode_url'])
            return item
        except Exception as e:
            print(e)
            #import pdb; pdb.set_trace()
            raise

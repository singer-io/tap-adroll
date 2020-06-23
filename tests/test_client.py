import os
import random
import base64
from datetime import datetime as dt

from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session

from tap_adroll.client import AdrollClient


class TestClient(AdrollClient):
    """ A client subclass to implement the data manipulation endpoints for the tap. """

    ADVERTISABLE_EID = "PWSMR23OXJGGLE4S3F5XI5"
    ADVERTISABLE_NAME = 'Advertiseable 1592412774.486537'

    # Track state for existing data to mimimize # of calls made
    ADVERTISABLES = []
    ADS = []
    AD_GROUPS = []
    SEGMENTS = []
    CAMPAIGNS = []


    def __init__(self):
        token = self.get_token_information()
        config = {
            'access_token': token['access_token'],
            'refresh_token': token['refresh_token'],
            'client_id': token['client_id'],
            'client_secret': token['client_secret'],
        }
        super().__init__('/dev/null', config)


    def _write_config(self, token):
        # Not needed
        pass

    @staticmethod
    def get_token_information():
        # Get a refresh token with password credentials
        client_id = os.getenv('TAP_ADROLL_CLIENT_ID')
        client_secret = os.getenv('TAP_ADROLL_CLIENT_SECRET')
        username = os.getenv('TAP_ADROLL_USERNAME')
        password = os.getenv('TAP_ADROLL_PASSWORD')

        oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
        return {"client_id": client_id,
                "client_secret": client_secret,
                **oauth.fetch_token(token_url='https://services.adroll.com/auth/token',
                                    username=username, password=password, client_id=client_id,
                                    client_secret=client_secret)}


    def get_all(self, stream, start_date=None, end_date=None):
        """dispatch function for geting all test data for a given stream"""
        if stream == 'advertisables':
            return self.get_all_advertisables()
        elif stream == 'ads':
            return self.get_all_ads()
        elif stream == 'ad_reports':
            if not start_date or not end_date:
                raise Exception("You must specify start and end date params.")
            return self.get_all_ad_reports(start_date, end_date)
        elif stream == 'campaigns':
            return self.get_all_campaigns()
        elif stream == 'segments':
            return self.get_all_segments()
        elif stream == 'ad_groups':
            return self.get_all_ad_groups()
        else:
            raise NotImplementedError
        

    def get_all_advertisables(self):
        if not self.ADVERTISABLES:
            self.ADVERTISABLES = self.get('organization/get_advertisables').get('results')
        return self.ADVERTISABLES

    def get_all_ads(self):
        if not self.ADS:
            adv_ids = []
            ads = []
            advertisables = self.get_all_advertisables()
            adv_ids = [adv.get('eid') for adv in advertisables]
            for adv in adv_ids:
                query_params = {'advertisable': adv}
                ads += self.get('advertisable/get_ads', params=query_params).get('results')
            self.ADS = ads
        return self.ADS

    def get_all_ad_reports(self, start_date, end_date):
        query_params = {'data_format':'entity',
                        'start_date':start_date,
                        'end_date':end_date}
        return self.get('/report/ad', params=query_params).get('results')

    def get_all_campaigns(self):
        if not self.CAMPAIGNS:
            adv_ids = []
            campaigns = []
            advertisables = self.get_all_advertisables()
            adv_ids = [adv.get('eid') for adv in advertisables]
            for adv in adv_ids:
                query_params = {'advertisable': adv}
                campaigns += self.get('advertisable/get_campaigns', params=query_params).get('results')
            self.CAMPAIGNS = campaigns
        return self.CAMPAIGNS

    def get_all_ad_groups(self):
        if not self.AD_GROUPS:
            cam_ids = []
            groups = []
            campaigns = self.get_all_campaigns()
            cam_ids = [cam.get('eid') for cam in campaigns]
            for cam in cam_ids:
                query_params = {'campaign': cam}
                groups += self.get('campaign/get_adgroups', params=query_params).get('results')
                self.AD_GROUPS = groups
        return self.AD_GROUPS

    def get_all_segments(self):
        if not self.SEGMENTS:
            adv_ids = []
            segments = []
            advertisables = self.get_all_advertisables()
            adv_ids = [adv.get('eid') for adv in advertisables]
            for adv in adv_ids:
                query_params = {'advertisable': adv}
                segments += self.get('advertisable/get_segments', params=query_params).get('results')
                self.SEGMENTS = segments
        return self.SEGMENTS

    def get_advertisables(self):
        response = self.get('advertisable/get')
        return response.get('results', response)


    def get_ads(self, advertisable_eid):
        query_params = {'advertisable': advertisable_eid}
        response = self.get('advertisable/get_ads', params=query_params)
        return response.get('results', response)


    def get_campaigns(self, advertisable_eid):
        query_params = {'advertisable': advertisable_eid}
        response = self.get('advertisable/get_campaigns', params=query_params)
        return response.get('results', response)


    def get_ad_groups(self, advertisable_eid):
        query_params = {'advertisable': advertisable_eid}
        response = self.get('advertisable/get_adgroups', params=query_params)
        return response.get('results', response)


    def get_segments(self, advertisable_eid):
        query_params = {'advertisable': advertisable_eid}
        response = self.get('advertisable/get_adgroups', params=query_params)


    def create(self, stream):
        if stream == 'advertisables':
            raise Exception("Creating {} objects can cause exponential increase in api calls".format(stream))
        elif stream == 'ads':
            return self.create_ad()
        elif stream == 'ad_reports':
            raise NotImplementedError
        elif stream == 'campaigns':
            return self.create_campaign()
        elif stream == 'segments':
            raise NotImplementedError  #return self.create_segment()
        elif stream == 'ad_groups':
           campaign_eid = random.choice(self.get_all_campaigns()).get('eid')
           return self.create_ad_group(campaign_eid)
        else:
            raise NotImplementedError

    def _get_abs_path(self, path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def _get_ad_file(self):
        size = random.choice(["315", "500", "600"])
        filename = 'stitch_loader_600x{}.png'.format(size)
        path = self._get_abs_path(filename)
        with open(path, "rb") as image_file:
            encoded_file = base64.b64encode(image_file.read())

        return encoded_file

    def create_ad(self):
        """
        create a static native ad
        https://developers.adroll.com/docs/guides/create-web-ads.html#native-ads
        """
        tstamp = str(dt.utcnow().timestamp())
        data = {
            'advertisable': self.ADVERTISABLE_EID,  # REQUIRED
            'type': 'native',  # string (‘image’) | Ad type
            'inventory_type': 'iab', # THERE IS NO DOCUMENTATION FOR THIS see link above
            'name': 'AD {}'.format(tstamp[:-7]),  # string (‘’) | name of the ad
            'body': 'I was created by test_client', # this is called Description in the UI
            'destination_url': 'http://thislemonadetateslikepotatoes{}.org'.format(tstamp[:-7]),  # string (‘’) | URL reached when ad is clicked
            'file': self._get_ad_file(),  # REQUIRED | base64-encoded string | actual contents of the ad
        }
        resp = self.post('ad/create', data=data)
        return resp.get('results')

    # def create_segment(self):
    #     tstamp = str(dt.utcnow().timestamp())
    #     data = {
    #         'advertisable': self.ADVERTISABLE_EID,
    #         'name': 'SEGMENT {}'.format(tstamp[:-7]),
    #         'conversion_value': None,
    #         'duration': random.randint(1,30),
    #         'type': 'p',
    #         # 'data': [
    #         #   {
    #         #     'email': '',
    #         #     'id': 'string'
    #         #   }
    #         # ],
    #         # 'general_exclusion_type': 'string',
    #         # 'is_conversion': False,
    #         # 'sfdc_company_list_id': 'string',
    #     }
    #     resp = self.post('/segments', data=data) # REQUIRES DIFFERENT BASE ENDPOINT
    #     return resp.get('results')

    def create_campaign(self):
        tstamp = dt.utcnow().timestamp()
        data = {
            'advertisable': self.ADVERTISABLE_EID,  # REQUIRED
            'budget': random.randint(1, 10),  # REQUIRED | number | The WEEKLY budget for the campaign
            'name': 'CAMPAIGN {}'.format(tstamp),  # string (‘’) | name of the campaign
        }
        resp = self.post('campaign/create', data=data)
        return resp.get('results')

    def create_ad_group(self, campaign_eid):
        tstamp = dt.utcnow().timestamp()
        data = {
            'campaign': campaign_eid,
            'name': 'AD GROUP {}'.format(tstamp),  # string (‘’) | name of the campaign
            'ads': None,  # array | A comma-separated list of Ad EIDs to attach to the adgroup (Optional; default: None)
            'positive_segments': None,  # array | A comma-separated list of Segment EIDs to attach to the adgroup as positive segments (Optional; default: None)
            'negative_segments': None, # array | A comma-separated list of Segment EIDs to attach to the adgroup as negative segments (Optional; default: None)
            'geo_targets': None,  # string | JSON string of desired geo targets for the adgroup The parsed JSON should be an array of objects, each object should be like
            'placement_targets': None # array | A JSON list of placements targets for Facebook ads. One of: all, rightcolumn, desktopfeed, mobilefeed, FAN, instagramstream (Optional; default: None)
        }
        resp = self.post('adgroup/create', data=data)
        return resp.get('results')


    def update(self, stream, eid=None):
        if stream == 'advertisables':
            raise Exception("Creating {} objects can cause exponential increase in api calls".format(stream))
        elif stream == 'ads':
            return self.update_ad(eid)
        elif stream == 'ad_reports':
            raise NotImplementedError
        elif stream == 'campaigns':
            return self.update_campaign(eid)
        elif stream == 'segments':
            raise NotImplementedError  #return self.create_segment()
        elif stream == 'ad_groups':
            return self.update_ad_group(eid)
        else:
            raise NotImplementedError

    def update_ad(self, eid):
        tstamp = str(dt.utcnow().timestamp())
        if eid is None:
            eid = random.choice(self.get_all_ads()).get('eid')
        data = {
            'ad': eid,
            'name': 'UPDATED AD {}'.format(tstamp[:-7]),
        }
        resp = self.put('ad/edit', data=data)
        return resp.get('results').get('original') # it says original but it is the updated ad

    def update_ad_group(self, eid):
        tstamp = str(dt.utcnow().timestamp())
        if eid is None:
            eid = random.choice(self.get_all_ad_groups()).get('eid')

        query_params = {'adgroup': eid}
        ads = self.get('adgroup/get', params=query_params).get('results').get('ads')

        data = {
            'adgroup': eid,
            'name': 'UPDATED AD GROUP {}'.format(tstamp[:-7]),
            'ads': ads,
        }

        resp = self.put('adgroup/edit', data=data)
        return resp.get('results')

    def update_campaign(self, eid):
        tstamp = str(dt.utcnow().timestamp())
        if eid is None:
            eid = random.choice(self.get_all_campaigns()).get('eid')
        data = {
            'campaign': eid,
            'name': 'UPDATED CAMPAIGN {}'.format(tstamp[:-7]),
            'ui_budget_daily': True
        }
        resp = self.put('campaign/edit', data=data)
        return resp.get('results')


    # NB: Commented create and deletes since AdRoll as of now, doesn't
    # seem to have a true "DELETE" in their CRUD

    # def delete_advertisable(self, advertisable_eid):
    #     resp = self.delete('advertisable/deactivate', data={'advertisable': advertisable_eid})
    #     return resp


    def get(self, url, headers=None, params=None, data=None):
        return self._make_request("GET", url, headers=headers, params=params)


    def post(self, url, headers=None, params=None, data=None):
        return self._make_request("POST", url, headers=headers, params=params, data=data)

    def put(self, url, headers=None, params=None, data=None):
        return self._make_request("PUT", url, headers=headers, params=params, data=data)

    def delete(self, url, headers=None, params=None, data=None):
        # Deleting as we've seen it thus far is a POST
        return self._make_request("POST", url, headers=headers, params=params, data=data)

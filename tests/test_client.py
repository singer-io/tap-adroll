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
        return self.get('organization/get_advertisables').get('results')

    def get_all_ads(self):
        adv_ids = []
        ads = []
        advertisables = self.get_all_advertisables()
        adv_ids = [adv.get('eid') for adv in advertisables]
        for adv in adv_ids:
            query_params = {'advertisable': adv}
            ads += self.get('advertisable/get_ads', params=query_params).get('results')
        return ads

    def get_all_ad_reports(self, start_date, end_date):
        query_params = {'data_format':'entity',
                        'start_date':start_date,
                        'end_date':end_date}
        return self.get('/report/ad', params=query_params).get('results')

    def get_all_campaigns(self):
        adv_ids = []
        campaigns = []
        advertisables = self.get_all_advertisables()
        adv_ids = [adv.get('eid') for adv in advertisables]
        for adv in adv_ids:
            query_params = {'advertisable': adv}
            campaigns += self.get('advertisable/get_campaigns', params=query_params).get('results')
        return campaigns

    def get_all_ad_groups(self):
        cam_ids = []
        groups = []
        campaigns = self.get_all_campaigns()
        cam_ids = [cam.get('eid') for cam in campaigns]
        for cam in cam_ids:
            query_params = {'campaign': cam}
            groups += self.get('campaign/get_adgroups', params=query_params).get('results')
        return groups

    def get_all_segments(self):
        adv_ids = []
        segments = []
        advertisables = self.get_all_advertisables()
        adv_ids = [adv.get('eid') for adv in advertisables]
        for adv in adv_ids:
            query_params = {'advertisable': adv}
            segments += self.get('advertisable/get_segments', params=query_params).get('results')
        return segments

    def get_advertisables(self):
        response = self.get('advertisable/get')
        return response.get('results', response)


    def get_ads(self, advertisable_eid):
        data = {'advertisable': advertisable_eid}
        response = self.get('advertisable/get_ads', data=data)
        return response.get('results', response)


    def get_campaigns(self, advertisable_eid):
        data = {'advertisable': advertisable_eid}
        response = self.get('advertisable/get_campaigns', data=data)
        return response.get('results', response)


    def get_ad_groups(self, advertisable_eid):
        data = {'advertisable': advertisable_eid}
        response = self.get('advertisable/get_adgroups', data=data)
        return response.get('results', response)


    def get_segments(self, advertisable_eid):
        data = {'advertisable': advertisable_eid}
        response = self.get('advertisable/get_adgroups', data=data)


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
        #     'ad_format': 34,  # 33 "Native Wide", 34 "Native Square"
        #     'message': 'This is an Ad?',  # string (‘’) | message text of the FB ad (Optional 500 char limit
        #     'brand_name': self.ADVERTISABLE_NAME,  # string ('') | brand name for native ads
        #     'display_url_override': 'http://thislemonadetateslikepotatoes{}.org'.format(tstamp[:-7]), #'http://thislemonadetasteslikepotatoes.org/',  # REQUIRED (if 'destination_url') | string ('') | final destination URL of the redirect
        #     'dynamic_template_id': '',  # string (‘’) | Dynamic Creative template to use
        #     'background': '',  # string ('white’) | Background color (hex value or name) or URL to an image for the Dynamic Creative ad
        #     'ad_format': '',  # string ('') | Ad format ID
        #     'prefix': '',  # string ('') | Product URLs will be prefixed with this when Dynamic Creative is clicked, used for redirect-style click trackers
        #     'tracking': '',  # string ('') | URL params to add to product URLs when Dynamic Creative is clicked
        #     # LIQUID ADS ONLY #############################################################################################
        #     'product': '',  # string ('') | The SWF data of the product animation loop
        #     'logo': '',  # string ('') | The data of the logo image
        #     # FACEBOOK ONLY ###############################################################################################
        #     'body': '',  # string (‘’) | body text of the FB ad (only FB ads and 90 char limit
        #     'headline': 'This is an ad?',  # string (‘’) | headline text of the FB ad (only for FB ads, 25 chars limit
        #     'headline_dynamic': '',  # string (‘’) | headline text of the FB ad
        #     'body_dynamic': '',  # string (‘’) | body text of the FB ad
        #     'message_dynamic': '',  # string (‘’) | message text of the FB ad
        #     'is_fb_dynamic': '',  # string (‘’) | True to indicate that this is a dynamic FB ad
        #     'multiple_products': '',  # integer  (0) [0, 3, 4, 5] | Number of products the FB ad should show
        #     'call_to_action': '',  # string (‘’) | CTA to use
        #     'lead_gen_form_id': '',  # string (‘’) | ID of the FB lead form for Lead Ads
        #     'multi_share_optimized': '',  # string (‘’) | True if FB should automatically select and order images for Carousel Ads 
        #     'child_ads': '',  # string (‘’) | Comma separated list of child ads for FB Carousel Ads
        #     'app_id': '',  # string  (‘’) | ID of application for FB App Ads
        # }
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
            # 'start_date': '',  # string | The day the campaign will start (Optional; default: tomorrow)
            # 'end_date': '',  # string | The day the campaign will end, exclusive. If None, then will run without end. (Optional; default: None)
            # 'adgroups': '',  # array | List of EIDs of adgroups to attach to this campaign (Optional; default: None)
            # 'ui_budget_daily': '', # boolean | Whether or not this campaign should show a daily budget in the UI. (Optional; default: true)
            # 'is_retargeting': '',  # boolean | Is this a retargeting campaign? Otherwise, false == geo campaign. (Optional; default: false)
            # 'cpc': '',  # number | The CPC goal for the campaign (Optional; default: None)
            # 'cpm': '',  # number | The CPM limit of the campaign, used in pricing model (Optional; default: None)
            # 'status': '',  # string | The status of the campaign. One of [‘admin_review’, ‘draft’] (Optional; default: admin_review)
            # 'max_cpm': '',  # number | The CPM limit for the networks, used in bidding (Optional; default: None)
            # # FACEBOOK ONLY ###############################################################################################
            # 'is_fbx_newsfeed': '',  # boolean | Is this a Facebook newsfeed campaign? Otherwise, false (Optional; default: false)
            # 'networks': '',  # string | A string of letters representing which networks to set up initially. Currently only supports ‘f’ (Facebook). (Optional; default: None)
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
            # str({  # ‘[{“country_id”:19,”eid”:”YD2QNVI2GVH4DP4TIO8GEO”,”is_negative”:false}, {“region_id”:”USCA”,”eid”:”FPDT2YVTEZG3LNMQ5Q8GEO”,”is_negative”:false}]’
            #     "country_id":19,  # one of: - “country_id” - “region_id” - “metro_id” - “city_id” - “postal_code_id” - “postal_code”
            #     "eid":"YD2QNVI2GVH4DP4TIO8GEO", # true identifier obtained from magellan about this geo_target
            #     "is_negative":False}),  # boolean, defaulting to False. When is_negative is true, that means the geolocation is excluded
            'placement_targets': None # array | A JSON list of placements targets for Facebook ads. One of: all, rightcolumn, desktopfeed, mobilefeed, FAN, instagramstream (Optional; default: None)
        }
        resp = self.post('adgroup/create', data=data)
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


    def delete(self, url, headers=None, params=None, data=None):
        # Deleting as we've seen it thus far is a POST
        return self._make_request("POST", url, headers=headers, params=params, data=data)

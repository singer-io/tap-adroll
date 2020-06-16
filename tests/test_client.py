import os

from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session

from tap_adroll.client import AdrollClient

class TestClient(AdrollClient):
    """ A client subclass to implement the data manipulation endpoints for the tap. """

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


    def get_all(self, stream, start_date=None):
        """dispatch function for geting all test data for a given stream"""
        if stream == 'advertisables':
            return self.get_all_advertisables()
        elif stream == 'ads':
            return self.get_all_ads()
        elif stream == 'ad_reports':
            return self.get_all_ad_reports(start_date)
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

    def get_all_ad_reports(self, start_date):
        query_params = {'data_format':'entity',
                        'start_date':start_date}
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


    # NB: Commented create and deletes since AdRoll as of now, doesn't
    # seem to have a true "DELETE" in their CRUD

    # def create_advertisable(self):
    #     # does our test account even let us do this?
    #     data = {'name': 'Test', 'organization': self.organization_eid, 'product_name': 'Testing Product'}
    #     resp = self.post('advertisable/create', data=data)
    #     return resp

    # def delete_advertisable(self, advertisable_eid):
    #     resp = self.delete('advertisable/deactivate', data={'advertisable': advertisable_eid})
    #     return resp


    # def create_campaign(self, advertisable_eid, budget=7):
    #     data = {'advertisable': advertisable_eid, 'budget': budget}
    #     resp = self.post('campaign/create', data=data)
    #     return resp


    # def create_ad_group(self, campaign_eid):
    #     data = {'campaign': campaign_eid, 'name': 'test adgroup'}
    #     resp = self.post('adgroup/create', data=data)
    #     return resp


    def get(self, url, headers=None, params=None, data=None):
        return self._make_request("GET", url, headers=headers, params=params)


    def post(self, url, headers=None, params=None, data=None):
        return self._make_request("POST", url, headers=headers, params=params, data=data)


    def delete(self, url, headers=None, params=None, data=None):
        # Deleting as we've seen it thus far is a POST
        return self._make_request("POST", url, headers=headers, params=params, data=data)
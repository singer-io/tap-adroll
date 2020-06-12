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
            'client_id': os.getenv('TAP_ADROLL_CLIENT_ID'),
            'client_secret': os.getenv('TAP_ADROLL_CLIENT_SECRET')
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
        return oauth.fetch_token(token_url='https://services.adroll.com/auth/token',
                                 username=username, password=password, client_id=client_id,
                                 client_secret=client_secret)


    def create_advertisable(self):
        # does our test account even let us do this?
        data = {'name': 'Test', 'organization': self.organization_eid, 'product_name': 'Testing Product'}
        resp = self.post('advertisable/create', data=data)
        return resp

    def delete_advertisable(self, advertisable_eid):
        resp = self.delete('advertisable/deactivate', data={'advertisable': advertisable_eid})
        return resp


    def post(self, url, headers=None, params=None, data=None):
        return self._make_request("POST", url, headers=headers, params=params, data=data)


    def delete(self, url, headers=None, params=None, data=None):
        # Deleting as we've seen it thus far is a POST
        return self._make_request("POST", url, headers=headers, params=params, data=data)
    

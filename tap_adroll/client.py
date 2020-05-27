import requests
from requests_oauthlib import OAuth2Session

import singer
import backoff

LOGGER = singer.get_logger()
ENDPOINT_BASE = "https://services.adroll.com/api/v1"

class AdrollClient():
    def __init__(self, config_path, config):
        token = {
            # access_token
            # refresh_token
            # token_type
            # expires_in -- I think we want to fudge this so it thinks its always expired on the first run / request
        }
        extra = {
            # client_id
            # client_secret
        }
        self.session = OAuth2Session(client_id, token=token, auto_refresh_url=refresh_url,
                                     auto_refresh_kwargs=extra, token_updater=_write_config)

        try:
            # Make an authenticated request after creating the object to any endpoint
        except:
            # raise out an exception for unauthenticated


    def _write_config(self):
        # Update config at config_path
        with open(self.config_path) as file:
            config = json.load(file)

        config['refresh_token'] = self.refresh_token
        config['access_token'] = self.access_token

        with open(self.config_path, 'w') as file:
            json.dump(config, file, indent=2)

    # Delete this function, I think
    def refresh(self):
        LOGGER.info("Refreshing credentials")

        auth_token = base64.b64encode((self.client_id + ':' + self.client_secret).encode()).decode()

        headers = {
            'Authorization': "Basic {}".format(auth_token),
            'Content-Type': 'application/x-www-form-urlencoded'
        }


        # Here
        url = "https://services.adroll.com/auth/token"

        body = 'grant_type=refresh_token&refresh_token={}'.format(self.refresh_token)
        request = requests.Request("POST", url, headers=headers, data=body)
        response = self.session.send(request.prepare())

        response = response.json()

        # to here.
        if response.get('error') == 'invalid_grant':
            raise XeroUnauthorized("Cannot authenticate")

        # Update local copies
        self.access_token = response['access_token']
        self.refresh_token = response['refresh_token']

        # Update config on filesystem
        self.write_config()

        
    @backoff.on_exception(backoff.constant,
                          (requests.exceptions.HTTPError),
                          max_tries=3,
                          interval=10)
    def _make_request(self, method, endpoint, headers=None, params=None):
        full_url = ENDPOINT_BASE + endpoint
        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            endpoint,
            params,
        )

        response = requests.request(method, full_url, headers=headers, params=params)

        response.raise_for_status()
        # TODO: Check error status, rate limit, etc.
        return response.json()

    def get(self, url, headers=None, params=None):
        return self._make_request("GET", url, headers=headers, params=params)

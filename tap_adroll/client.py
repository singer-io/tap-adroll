import json
import backoff
import requests
import singer

from requests_oauthlib import OAuth2Session

LOGGER = singer.get_logger()
ENDPOINT_BASE = "https://services.adroll.com/api/v1/"
TOKEN_REFRESH_URL = 'https://services.adroll.com/auth/token'


class AdrollAuthenticationError(Exception):
    pass
class AdrollClient():
    def __init__(self, config_path, config, dev_mode = False):
        self.dev_mode = dev_mode
        self.config_path = config_path
        self.config = config

        self.authenticate_request()
        try:
            # Make an authenticated request after creating the object to any endpoint
            org = self.get('organization/get').get('results', {}).get('eid')
            self.organization_eid = org
        except Exception as e:
            LOGGER.info("Error initializing AdrollClient during token refresh, please reauthenticate.")
            raise AdrollAuthenticationError(e)

    def authenticate_request(self):
        token = {
            'access_token': self.config['access_token'],
            'refresh_token': self.config['refresh_token'],
            'token_type': 'Bearer',
            # Set expires_in to a negative number to force the client to reauthenticate
            'expires_in': '-30'
        }
        extra = {
            'client_id': self.config['client_id'],
            'client_secret': self.config['client_secret']
        }
        if self.dev_mode :
            if not self.config.get('access_token'):
                raise Exception("Access token config property is missing")

            dev_mode_token = {
                "refresh_token": self.config.get('refresh_token'),
                # Using the existing access_token for dev mode
                "access_token": self.config.get('access_token'),
                'token_type': 'Bearer'
            }

            self.session = OAuth2Session(self.config['client_id'],
                                         token=dev_mode_token)
        else :

            self.session = OAuth2Session(self.config['client_id'],
                                         token=token,
                                         auto_refresh_url=TOKEN_REFRESH_URL,
                                         auto_refresh_kwargs=extra,
                                         token_updater=self._write_config)

    def _write_config(self, token):
        LOGGER.info("Credentials Refreshed")
        # Update config at config_path
        with open(self.config_path) as file:
            config = json.load(file)

        config['refresh_token'] = token['refresh_token']
        config['access_token'] = token['access_token']

        with open(self.config_path, 'w') as file:
            json.dump(config, file, indent=2)

    @backoff.on_exception(backoff.constant,
                          (requests.exceptions.HTTPError),
                          max_tries=3,
                          interval=10)
    def _make_request(self, method, endpoint, headers=None, params=None, data=None, override_api=None):
        full_url = ENDPOINT_BASE + endpoint
        if override_api:
            full_url = full_url.replace('api', override_api)

        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            endpoint,
            params,
        )

        # TODO: We should merge headers with some default headers like user_agent
        response = self.session.request(method, full_url, headers=headers, params=params, data=data)
        response.raise_for_status()
        # TODO: Check error status, rate limit, etc.
        return response.json()

    def get(self, url, headers=None, params=None):
        return self._make_request("GET", url, headers=headers, params=params)

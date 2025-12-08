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
            config_file = json.load(file)

        config_file['refresh_token'] = token['refresh_token']
        config_file['access_token'] = token['access_token']

        self.config['access_token'] = token['access_token']
        self.config['refresh_token'] = token['refresh_token']

        with open(self.config_path, 'w') as file:
            json.dump(config_file, file, indent=2)

    def _refresh_token(self):
        """
        Refresh the access token using the refresh token.
        Updates the session with new credentials and writes them to config.

        Raises:
            Exception: If token refresh fails
        """
        try:
            LOGGER.info("Attempting to refresh access token...")
            # Use OAuth2Session's refresh_token method to get new credentials
            new_token = self.session.refresh_token(
                TOKEN_REFRESH_URL,
                refresh_token=self.config['refresh_token'],
                client_id=self.config['client_id'],
                client_secret=self.config['client_secret']
            )
            LOGGER.info("Token refreshed successfully. Updating config...")
            # Update config file and in-memory config with new tokens
            self._write_config(new_token)
            return new_token
        except Exception as refresh_err:
            LOGGER.error("Failed to refresh token: %s", refresh_err)
            raise

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

        try:
            # TODO: We should merge headers with some default headers like user_agent
            response = self.session.request(method, full_url, headers=headers, params=params, data=data)
            response.raise_for_status()
            # TODO: Check error status, rate limit, etc.
            return response.json()
        except requests.exceptions.HTTPError as err:
            status = getattr(err.response, "status_code", None)
            if (status in (401, 403)) and not self.dev_mode:
                LOGGER.info("Auth error (%s). Attempting manual token refresh.", status)
                self._refresh_token()
                # Retry the request with the newly refreshed token
                retry_resp = self.session.request(method, full_url, headers=headers, params=params, data=data)
                retry_resp.raise_for_status()
                return retry_resp.json()
            raise

    def get(self, url, headers=None, params=None):
        return self._make_request("GET", url, headers=headers, params=params)

import json
from requests.exceptions import HTTPError

import unittest
from unittest.mock import patch, MagicMock

from tap_adroll.client import AdrollClient


class Test_ClientDevMode(unittest.TestCase):
    """Test the dev mode functionality."""
 
    # Data to be written
    mock_config = {
    "access_token": "sample_access_token",
    "refresh_token": "sample_refresh_token",
    "client_id": "sample_client_id",
    "client_secret": "sample_client_secret"
    }
    tmp_config_filename = "adroll_config.json"
    # Serializing json
    json_object = json.dumps(mock_config, indent=4)
    
    # Writing to sample.json
    with open(tmp_config_filename, "w") as outfile:
        outfile.write(json_object)

    @patch("requests.Session.request", return_value=MagicMock(return_value={'results': {'eid': 12345}}, status_code=200))
    def test_client_with_dev_mode(self, mock_request):
        """Checks the dev mode implementation works with existing token"""

        params = {"config_path": self.tmp_config_filename, "config": self.mock_config, "dev_mode": True}

        client = AdrollClient(**params)
        client.authenticate_request()

        headers = {}
        headers['Authorization'] = f"Bearer {self.mock_config['access_token']}"

        mock_request.assert_called_with("GET", "https://services.adroll.com/api/v1/organization/get", headers=headers, params=None, data=None)

    @patch("requests_oauthlib.OAuth2Session.request", return_value=MagicMock(return_value={'results': {'eid': 12345}}, status_code=200))
    def test_client_without_dev_mode(self, mock_request):
        """Checking the code flow without dev mode"""

        params = {"config_path": self.tmp_config_filename, "config": self.mock_config, "dev_mode": False}

        client = AdrollClient(**params)
        client.authenticate_request()

        mock_request.assert_called_with("GET", "https://services.adroll.com/api/v1/organization/get", headers=None, params=None, data=None)

    @patch('tap_adroll.client.requests.Session.request')
    @patch('tap_adroll.client.AdrollClient.get')
    def test_request_timeout_and_backoff_on_make_request(self, mock_get, mock_send):
        """
        Check whether the request backoffs properly for 3 times in case of HTTPError.
        """
        mock_send.side_effect = HTTPError
        params = {"config_path": self.tmp_config_filename, "config": self.mock_config, "dev_mode": True}
        client = AdrollClient(**params)
        with self.assertRaises(HTTPError):
            client._make_request('GET', 'organization/get')
        self.assertEquals(mock_send.call_count, 3)
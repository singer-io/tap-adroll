import os
import unittest

import tap_tester.connections as connections
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session


class TestAdrollBase(unittest.TestCase):
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def setUp(self):
        missing_envs = [x for x in [
            "TAP_ADROLL_REFRESH_TOKEN",
            "TAP_ADROLL_CLIENT_ID",
            "TAP_ADROLL_CLIENT_SECRET",
        ] if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    @staticmethod
    def get_type():
        return "platform.adroll"

    @staticmethod
    def tap_name():
        return "tap-adwords"

    @staticmethod
    def get_properties():
        return {
            'start_date' : '2020-03-01T00:00:00Z'
        }

    @staticmethod
    def get_credentials():
        # Get a refresh token with password credentials
        client_id = os.getenv('TAP_ADROLL_CLIENT_ID')
        client_secret = os.getenv('TAP_ADROLL_CLIENT_SECRET')
        username = os.getenv('TAP_ADROLL_USERNAME')
        password = os.getenv('TAP_ADROLL_PASSWORD')

        oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
        token = oauth.fetch_token(token_url='https://services.adroll.com/auth/token',
                                  username=username, password=password, client_id=client_id,
                                  client_secret=client_secret)
        return {
            'refresh_token': token['refresh_token'],
            'client_id': client_id,
            'client_secret': client_secret,
            'access_token': token['access_token']
        }

    @staticmethod
    def expected_check_streams():
        return {
            'advertisables',
            'ads',
            'ad_reports',
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        return {
            "advertisables": {
                self.PRIMARY_KEYS: {'eid'},
                self.REPLICATION_METHOD: self.FULL,
            },
            "ads": {
                self.PRIMARY_KEYS: {'eid'},
                self.REPLICATION_METHOD: self.FULL,
            },
            "ad_reports": {
                self.PRIMARY_KEYS: {'eid', 'date'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'date'},
            },
        }

    def expected_replication_method(self):
        """return a dictionary with key of table name and value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

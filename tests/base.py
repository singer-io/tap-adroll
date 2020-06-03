import tap_tester.connections as connections
import os
import unittest

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
        ] if os.getenv(x) == None]
        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def get_type(self):
        return "platform.adroll"

    def tap_name(self):
        return "tap-adwords"

    def get_properties(self):
        return {
            'start_date' : '2020-03-01T00:00:00Z'
        }

    def get_credentials(self):
        return {
            'refresh_token': os.getenv('TAP_ADROLL_REFRESH_TOKEN'),
            'client_id': os.getenv('TAP_ADROLL_CLIENT_ID'),
            'client_secret': os.getenv('TAP_ADROLL_CLIENT_SECRET'),
            'access_token': 'fake'
        }

    def expected_check_streams(self):
        return {
            'advertisables',
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        return {
            "advertisables": {
                self.PRIMARY_KEYS: {'eid'},
                self.REPLICATION_METHOD: self.FULL,
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

    def preserve_refresh_token(self, existing_conns, payload):
        if not existing_conns:
            return payload
        conn_with_creds = connections.fetch_existing_connection_with_creds(existing_conns[0]['id'])
        payload['properties']['refresh_token'] = conn_with_creds['credentials']['refresh_token']
        return payload

import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import tap_tester.menagerie   as menagerie
import tap_tester.connections as connections

from test_client import TestClient


class TestAdrollBase(unittest.TestCase):
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z" # %H:%M:%SZ
    REPORTS_START_DATE = "2016-06-02T00:00:00Z" # test data for ad_reports is static
    REPORTS_END_DATE = "2016-06-06T00:00:00Z"

    def setUp(self):
        missing_envs = [x for x in [
            "TAP_ADROLL_CLIENT_ID",
            "TAP_ADROLL_CLIENT_SECRET",
            "TAP_ADROLL_USERNAME",
            "TAP_ADROLL_PASSWORD"
        ] if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    @staticmethod
    def get_type():
        return "platform.adroll"

    @staticmethod
    def tap_name():
        return "tap-adroll"

    def get_properties(self, original: bool = True):
        """
        Maintain states for start_date and end_date
        :param original: set to false to change the start_date or end_date
        """
        return_value = {
            'start_date' : dt.strftime(dt.utcnow()-timedelta(days=5), self.START_DATE_FORMAT),
            'end_date' : dt.strftime(dt.utcnow(), self.START_DATE_FORMAT)
        }
        if original:
            return return_value

        assert self.END_DATE > self.START_DATE, "You can't set end date prior to start date."

        # Reassign start and end dates
        return_value["start_date"] = self.START_DATE
        return_value["end_date"] = self.END_DATE
        return return_value

    def get_credentials(self):
        token = TestClient.get_token_information()
        return {
            'refresh_token': token['refresh_token'],
            'client_id': token['client_id'],
            'client_secret': token['client_secret'],
            'access_token': token['access_token']
        }

    @staticmethod
    def expected_check_streams():
        return {
            'advertisables',
            'ads',
            'ad_groups',
            'ad_reports',
            'campaigns',
            'segments',
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
            "ad_groups": {
                self.PRIMARY_KEYS: {'eid'},
                self.REPLICATION_METHOD: self.FULL,
            },
            "ad_reports": {
                self.PRIMARY_KEYS: {'eid', 'date'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'date'},
            },
            "campaigns": {
                self.PRIMARY_KEYS: {'eid'},
                self.REPLICATION_METHOD: self.FULL,
            },
            "segments": {
                self.PRIMARY_KEYS: {'eid'},
                self.REPLICATION_METHOD: self.FULL,
            },
        }

    def expected_replication_method(self):
        """return a dictionary with key of table name and value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_incremental_streams(self):
        return set(stream for stream, rep_meth in self.expected_replication_method().items()
                   if rep_meth == self.INCREMENTAL)

    def expected_full_table_streams(self):
        return set(stream for stream, rep_meth in self.expected_replication_method().items()
                   if rep_meth == self.FULL)

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

    def expected_foreign_keys(self):
        """
        return dictionary with key of table name and
        value is set of foreign keys
        """
        return {  # TODO add foreign keys if found
            "advertiseables": set(),
            "ads": set(),
            "advertiseables": set()
        }

    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True, exclude_streams=[]):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            if exclude_streams and catalog.get('stream_name') in exclude_streams:
                continue
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {})
                # remove properties that are automatic
                for prop in self.expected_automatic_fields().get(catalog['stream_name'], []):
                    if prop in non_selected_properties:
                        del non_selected_properties[prop]
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties
            )

    def _get_abs_path(self, path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def _load_schemas(self, stream):
        schemas = {}

        path = self._get_abs_path("/schemas") + "/" + stream + ".json"
        final_path = path.replace('tests', 'tap_adroll')
        
        with open(final_path) as file:
            schemas[stream] = json.load(file)

        return schemas

    def expected_schema_keys(self, stream):
        import pdb; pdb.set_trace()
        props = self._load_schemas(stream).get('properties')
        assert props, "{} schema not configured proprerly"

        return props.keys()

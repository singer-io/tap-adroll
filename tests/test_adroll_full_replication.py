import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest

class TestAdrollFullReplication(unittest.TestCase):
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

    def name(self):
        return "tap_tester_adroll_full_replication"

    def get_type(self):
        return "platform.adroll"

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

    def tap_name(self):
        return "tap-adwords"


    def get_properties(self):
        return {
            'start_date' : '2020-03-01T00:00:00Z'
        }

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    def run_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        return sync_record_count

    def test_run(self):
        """
        Verify that a bookmark doesn't exist for the stream
        Verify that the second sync includes the same number or more records than the first sync
        Verify that all records in the first sync are included in the second sync
        Verify that the sync only sent records to the target for selected streams (catalogs)

        PREREQUISITE
        For EACH stream that is fully replicated there are multiple rows of data with
            different values for the replication key
        """
        def preserve_refresh_token(existing_conns, payload):
            if not existing_conns:
                return payload
            conn_with_creds = connections.fetch_existing_connection_with_creds(existing_conns[0]['id'])
            payload['properties']['refresh_token'] = conn_with_creds['credentials']['refresh_token']
            return payload

        conn_id = connections.ensure_connection(self, payload_hook=preserve_refresh_token)

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        full_streams = {key for key, value in self.expected_replication_method().items()
                        if value == self.FULL}
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in full_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), full_streams)

        first_sync_state = menagerie.get_state(conn_id)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        # Run a second sync job using orchestrator
        second_sync_record_count = runner.run_sync_mode(self, conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        # THIS MAKES AN ASSUMPTION THAT CHILD STREAMS DO NOT NEED TESTING.
        # ADJUST IF NECESSARY
        for stream in full_streams.difference(self.child_streams()):
            with self.subTest(stream=stream):

                # verify there is no bookmark values from state
                state_value = first_sync_state.get("bookmarks", {}).get(stream)
                self.assertIsNone(state_value)

                # verify that there is more than 1 record of data - setup necessary
                self.assertGreater(first_sync_record_count.get(stream, 0), 1,
                                   msg="Data isn't set up to be able to test full sync")

                # verify that you get the same or more data the 2nd time around
                self.assertGreaterEqual(
                    second_sync_record_count.get(stream, 0),
                    first_sync_record_count.get(stream, 0),
                    msg="second syc didn't have more records, full sync not verified")

                # verify all data from 1st sync included in 2nd sync
                first_data = [record["data"] for record
                              in first_sync_records.get(stream, {}).get("messages", {"data": {}})]
                second_data = [record["data"] for record
                               in second_sync_records.get(stream, {}).get("messages", {"data": {}})]

                same_records = 0
                for first_record in first_data:
                    first_value = json.dumps(first_record, sort_keys=True)

                    for compare_record in second_data:
                        compare_value = json.dumps(compare_record, sort_keys=True)

                        if first_value == compare_value:
                            second_data.remove(compare_record)
                            same_records += 1
                            break

                self.assertEqual(len(first_data), same_records,
                                 msg="Not all data from the first sync was in the second sync")

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import unittest
import simplejson

from base import TestAdrollBase
from test_client import TestClient

class TestAdrollIncrementalReplication(TestAdrollBase):
    def name(self):
        return "tap_tester_adroll_incremental_replication"

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema)

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


    def setUp(self):
        pass

    def tearDown(self):
        pass


    def test_run(self):
        """
        Verify for each stream that you can do a sync which records bookmarks.
        Verify that the bookmark is the max value sent to the target for the `date` PK field
        Verify that the 2nd sync respects the bookmark
        Verify that all data of the 2nd sync is >= the bookmark from the first sync
        Verify that the number of records in the 2nd sync is less then the first
        Verify inclusivivity of bookmarks

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), incremental_streams,
                         msg="Expect first_sync_record_count keys {} to equal incremental_streams {},"
                         " first_sync_record_count was {}".format(
                             first_sync_record_count.keys(),
                             incremental_streams,
                             first_sync_record_count))

        first_sync_state = menagerie.get_state(conn_id)

        # Verify the state against the end_date
        for stream in incremental_streams:
            replication_key = next(iter(self.expected_metadata().get(stream).get(self.REPLICATION_KEYS)))
            d1 = first_sync_state.get('bookmarks').get(stream).get(replication_key)
            d2 = self.get_properties().get('end_date')
            self.assertEqual(d1.split('T')[0], d2.split('T')[0])

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        second_sync_state = menagerie.get_state(conn_id)
        
        # Loop first_sync_records and compare against second_sync_records
        # each iteration of loop is chekcing both for a stream
        # first_sync_records["ads"] == second["ads"]
        for stream in incremental_streams:
            with self.subTest(stream=stream):

                # verify both syncs write / keep the same bookdmark
                self.assertEqual(first_sync_state, second_sync_state)

                # verify that there is more than 1 record of data - setup necessary
                self.assertGreater(first_sync_record_count.get(stream, 0), 1,
                                   msg="Data isn't set up to be able to test full sync")

                # verify that you get less data on the 2nd sync
                self.assertGreaterEqual(
                    first_sync_record_count.get(stream, 0),
                    second_sync_record_count.get(stream, 0),
                    msg="first syc didn't have more records, bookmark usage not verified")

                # Verify that all data of the 2nd sync is >= the bookmark from the first sync
                second_data = [record["data"]["date"] for record
                               in second_sync_records.get(stream, {}).get("messages", {"data": {}})]

                replication_key = next(iter(self.expected_metadata().get(stream).get(self.REPLICATION_KEYS)))
                first_sync_bookmark = first_sync_state.get('bookmarks').get(stream).get(replication_key).split('T')[0]
                for date_value in second_data:
                    
                    self.assertEqual(first_sync_bookmark,
                                     date_value.split('T')[0],
                                     msg="First sync bookmark does not equal 2nd sync record's replication-key")

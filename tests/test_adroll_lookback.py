import unittest
import simplejson
from datetime import datetime as dt
import datetime
from singer import utils

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestAdrollBase
from test_client import TestClient

class TestAdrollIncrementalReplication(TestAdrollBase):
    START_DATE = ""
    END_DATE = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"


    def name(self):
        return "tap_tester_adroll_incremental_replication"

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

    @classmethod
    def setUpClass(cls):
        print("\n\nTEST SETUP\n")
        cls.client = TestClient()

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")


    def test_run(self):
        """
        Verify that you can do a sync that starts with a bookmark that is earlier than now() - lookback and the first record returned needs to have a date value == bookmark.
        Verify that you can do a sync that starts with a bookmark that is later than now() - lookback and the first record returned needs to have a date value == now - lookback.
        Verify that you can do a sync that starts with a bookmark later than non-default lookback and first record returned needs to have a date value == now() - non-default lookback.

        PREREQUISITE
        For EACH stream that uses the lookback_window there are multiple rows of data with
            different values for the replication key
        """
        # Overriding start/end dates and defining lookback date
        self.END_DATE = dt.strftime(utils.now(), self.START_DATE_FORMAT)
        START_DATE_BOOKMARK_PRE_LOOKBACK = utils.now() - datetime.timedelta(days=10)
        self.expected_date = dt.strftime(START_DATE_BOOKMARK_PRE_LOOKBACK, "%Y-%m-%dT00:00:00.000000Z")
        self.START_DATE = dt.strftime(START_DATE_BOOKMARK_PRE_LOOKBACK, self.START_DATE_FORMAT)
        with self.subTest(start_date=START_DATE_BOOKMARK_PRE_LOOKBACK):
            self.run_tap_with_config()

        START_DATE_BOOKMARK_POST_LOOKBACK = utils.now() - datetime.timedelta(days=3)
        self.expected_date = dt.strftime(utils.now() - datetime.timedelta(days=7), "%Y-%m-%dT00:00:00.000000Z")
        self.START_DATE = dt.strftime(START_DATE_BOOKMARK_POST_LOOKBACK, self.START_DATE_FORMAT)
        with self.subTest(start_date=START_DATE_BOOKMARK_POST_LOOKBACK):
            self.run_tap_with_config()

        START_DATE_BOOKMARK_POST_LOOKBACK = utils.now() - datetime.timedelta(days=3)
        LOOKBACK_DATE = utils.now() - datetime.timedelta(days=10)
        self.expected_date = dt.strftime(LOOKBACK_DATE, "%Y-%m-%dT00:00:00.000000Z")
        self.START_DATE = dt.strftime(START_DATE_BOOKMARK_POST_LOOKBACK, self.START_DATE_FORMAT)
        self.LOOKBACK_WINDOW = "10"
        with self.subTest(lookback_window=self.LOOKBACK_WINDOW):
            self.run_tap_with_config()

    def run_tap_with_config(self):

        # Instantiate connection with non-default start/end dates
        conn_id = connections.ensure_connection(self, original_properties=False)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Select all incremental streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        lookback_streams = self.expected_lookback_streams()
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in lookback_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        for stream in lookback_streams:
            # Get the 'date' of the first record returned in the first sync
            first_sync_first_date = min(
                [record.get("data", {}).get("date") for record
                 in first_sync_records.get(stream, {}).get("messages", {"data": {}})])

            # Verify that the 'date' of the first record == expected_date
            self.assertEqual(first_sync_first_date, self.expected_date,
                             msg="First date did not match expected date")

if __name__ == '__main__':
    unittest.main()

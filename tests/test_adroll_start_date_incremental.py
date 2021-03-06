 
import unittest
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestAdrollBase
from test_client import TestClient


class TestAdrollStartDateIncremental(TestAdrollBase):
    START_DATE = ""
    END_DATE = ""
    

    def name(self):
        return "tap_tester_adroll_start_date_incremental"

    def testable_streams(self):
        return self.expected_incremental_streams().difference(
            set()# STREAMS THAT CANNOT CURRENTLY BE TESTED
        )

    @classmethod
    def setUpClass(cls):
        print("\n\nTEST SETUP\n")
        cls.client = TestClient()

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")


    def test_run(self):
        print("\n\nRUNNING {}\n\n".format(self.name()))

        # Initialize start_date state to make assertions
        self.START_DATE = self.timedelta_formatted(self.REPORTS_START_DATE, -1)
        start_date_1 = self.START_DATE
        start_date_2 = self.timedelta_formatted(self.START_DATE, 1)  # + 1 days
        self.END_DATE = self.REPORTS_END_DATE
        print("INCREMENTAL STREAMS RELY ON A STATIC DATA SET. SO WE TEST WITH:\n" +
              "  START DATE 1 | {}".format(start_date_1) +
              "  START DATE 2 | {}".format(start_date_2) +
              "  END DATE 2 | {}".format(self.END_DATE))

        # get expected records
        expected_records_1 = {x: [] for x in self.expected_streams()} # ids by stream
        for stream in self.testable_streams():
            start_date = self.parse_date(self.START_DATE)
            end_date = self.parse_date(self.END_DATE)
            existing_objects = self.client.get_all(stream, start_date, end_date)
            assert existing_objects, "Test data is not properly set for {}, test will fail.".format(stream)
            print("Data exists for stream: {}".format(stream))
            for obj in existing_objects:
                expected_records_1[stream].append(obj)

        ##########################################################################
        ### First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(self, original_properties=False)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        # Select all available fields from testable streams
        exclude_streams = list(self.expected_streams().difference(self.testable_streams()))
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=found_catalogs,
                                           exclude_streams=exclude_streams)

        catalogs = menagerie.get_catalogs(conn_id)

        #clear state
        menagerie.set_state(conn_id, {})

        # Run sync 1
        sync_job_1 = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status_1 = menagerie.get_exit_status(conn_id, sync_job_1)
        menagerie.verify_sync_exit_status(self, exit_status_1, sync_job_1)

        # read target output
        record_count_by_stream_1 = runner.examine_target_output_file(self, conn_id,
                                                                     self.expected_streams(), self.expected_primary_keys())
        replicated_row_count_1 =  reduce(lambda accum,c : accum + c, record_count_by_stream_1.values())
        self.assertGreater(replicated_row_count_1, 0, msg="failed to replicate any data: {}".format(record_count_by_stream_1))
        print("total replicated row count: {}".format(replicated_row_count_1))
        synced_records_1 = runner.get_records_from_target_output()

        state_1 = menagerie.get_state(conn_id)

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        self.START_DATE = start_date_2
        print("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(start_date_1, start_date_2))
        self.END_DATE= self.get_properties(False)['end_date']

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id = connections.ensure_connection(self, original_properties=False)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are kosher")

        # Select all available streams and their fields
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=found_catalogs, exclude_streams=exclude_streams)

        catalogs = menagerie.get_catalogs(conn_id)

        # clear state
        menagerie.set_state(conn_id, {})

        # Run sync 2
        sync_job_2 = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status_2 = menagerie.get_exit_status(conn_id, sync_job_2)
        menagerie.verify_sync_exit_status(self, exit_status_2, sync_job_2)

        # This should be validating the the PKs are written in each record
        record_count_by_stream_2 = runner.examine_target_output_file(self, conn_id,
                                                                     self.expected_streams(), self.expected_primary_keys())
        replicated_row_count_2 =  reduce(lambda accum,c : accum + c, record_count_by_stream_2.values(), 0)
        self.assertGreater(replicated_row_count_2, 0, msg="failed to replicate any data")
        print("total replicated row count: {}".format(replicated_row_count_2))

        synced_records_2 = runner.get_records_from_target_output()

        state_2 = menagerie.get_state(conn_id)

        for stream in self.testable_streams():
            with self.subTest(stream=stream):
                replication_type = self.expected_replication_method().get(stream)
                replication_key = next(iter(self.expected_metadata().get(stream).get(self.REPLICATION_KEYS)))
                record_count_1 = record_count_by_stream_1.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)

                # Testing how INCREMENTAL streams handle start date
                if replication_type == self.INCREMENTAL:

                    # Verify 1st sync record count > 2nd sync record count since the 1st start date is older than the 2nd.
                    self.assertGreater(replicated_row_count_1, replicated_row_count_2, msg="Expected less records on 2nd sync.")

                    # Verify that each stream has less records in 2nd sync than the 1st.
                    self.assertLess(record_count_2, record_count_1,
                                    msg="\nStream '{}' is {}\n".format(stream, self.INCREMENTAL) +
                                     "Record count 2 should be less than 2, but is not\n" +
                                     "Sync 1 start_date: {} ".format(start_date_1) +
                                     "Sync 1 record_count: {}\n".format(record_count_1) +
                                     "Sync 2 start_date: {} ".format(start_date_2) +
                                     "Sync 2 record_count: {}".format(record_count_2))

                    # Verify all data from first sync has bookmark values >= start_date .
                    records_from_sync_1 = set(row.get('data').get(replication_key) #.get('created_date')
                                              for row in synced_records_1.get(stream, []).get('messages', []))
                    for record in records_from_sync_1:
                        self.assertGreaterEqual(self.parse_date(record), self.parse_date(start_date_1),
                                                msg="Record was created prior to start date for 1st sync.\n" +
                                                "Sync 1 start_date: {}\n".format(start_date_1) +
                                                "Record bookmark: {} ".format(record))

                    # Verify all data from second sync has bookmark values >= start_date 2.
                    records_from_sync_2 = set(row.get('data').get(replication_key) # .get('created_date')
                                              for row in synced_records_2.get(stream, []).get('messages', []))
                    for record in records_from_sync_2:
                        self.assertGreaterEqual(self.parse_date(record), self.parse_date(start_date_2),
                                                msg="Record was created prior to start date for 2nd sync.\n" +
                                                "Sync 2 start_date: {}\n".format(start_date_2) +
                                                "Record bookmark: {} ".format(record))

                else:
                    raise Exception("Expectations are set incorrectly. {} cannot have a "
                                    "replication method of {}".format(stream, replication_type))


if __name__ == '__main__':
    unittest.main()


import unittest
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestAdrollBase
from test_client import TestClient


class TestAdrollStartDateFullTable(TestAdrollBase):
    START_DATE = ""
    END_DATE = ""


    def name(self):
        return "tap_tester_adroll_start_date_full_table"

    def testable_streams(self):
        return self.expected_full_table_streams().difference({
            'advertisables',  # STREAMS THAT CANNOT CURRENTLY BE TESTED
        })

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
        self.START_DATE = self.get_properties().get('start_date')
        start_date_1 = self.START_DATE  # default
        start_date_2 = self.timedelta_formatted(self.START_DATE, 2)  # default + 2 days

        # get expected records
        expected_records_1 = {x: [] for x in self.expected_streams()} # ids by stream
        for stream in self.testable_streams():
            existing_objects = self.client.get_all(stream)
            assert existing_objects, "Test data is not properly set for {}, test will fail.".format(stream)
            print("Data exists for stream: {}".format(stream))
            for obj in existing_objects:
                expected_records_1[stream].append(obj)

            # If no objects exist since the 2nd start_date, create one
            data_in_range = False
            for obj in expected_records_1.get(stream):
                created = obj.get('created_date').replace(' ', 'T', 1) + 'Z' # 2016-06-02 19:57:10 -->> 2016-06-02T19:57:10Z
                assert created, "'created_date' is not an attribute of {}".format(obj.get('name'))
                if self.parse_date(created) > self.parse_date(start_date_2):
                    data_in_range = True
                    break
            if not data_in_range:
                if stream in self.testable_streams():
                    expected_records_1[stream].append(self.client.create(stream))
                    continue
                assert None, "Sufficient test data does not exist for {}, test will fail.".format(stream)

        ##########################################################################
        ### First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(self)

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
        
        # Select all available streams and their fields
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=found_catalogs)

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
        self.END_DATE= self.get_properties()['end_date']

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
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=found_catalogs)

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
                record_count_1 = record_count_by_stream_1.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)

                # Testing how FULL TABLE streams handle start date
                if replication_type == self.FULL:

                    # Verify that a bookmark doesn't exist for the stream.
                    self.assertTrue(state_1.get(stream) is None,
                                    msg="There should not be bookmark value for {}\n{}".format(stream, state_1.get(stream)))
                    self.assertTrue(state_2.get(stream) is None,
                                    msg="There should not be bookmark value for {}\n{}".format(stream, state_1.get(stream)))

                    # Verify that the 2nd sync includes the same number of records as the 1st sync.
                    # -> Currently full table does not obey start_date, which makes this assertion valid
                    self.assertEqual(record_count_2, record_count_1,
                                     msg="\nStream '{}' is {}\n".format(stream, self.FULL) +
                                     "Record counts should be equal, but are not\n" +
                                     "Sync 1 start_date: {} ".format(start_date_1) +
                                     "Sync 1 record_count: {}\n".format(record_count_1) +
                                     "Sync 2 start_date: {} ".format(start_date_2) + 
                                     "Sync 2 record_count: {}".format(record_count_2))

                    # Verify all records in the 1st sync are included in the 2nd sync since
                    # 2nd sync has a later start date.
                    records_from_sync_1 = set(row.get('data').get('eid')
                                              for row in synced_records_1.get(stream, []).get('messages', []))
                    records_from_sync_2 = set(row.get('data').get('eid')
                                              for row in synced_records_2.get(stream, []).get('messages', []))
                    self.assertEqual(set(), records_from_sync_1.difference(records_from_sync_2),
                                     msg="Sync 2 record(s) missing from Sync 1:\n{}".format(
                                         records_from_sync_2.difference(records_from_sync_1))
                    )

                else:
                    raise Exception("Expectations are set incorrectly. {} cannot have a "
                                    "replication method of {}".format(stream, replication_type))
                    

if __name__ == '__main__':
    unittest.main()

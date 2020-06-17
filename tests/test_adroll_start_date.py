import os
import unittest
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestAdrollBase
from test_client import TestClient


class TestAdrollStartDate(TestAdrollBase):
    START_DATE = ""
    MIDNIGHT_FORMAT = "%Y-%m-%dT00:00:00Z"
    
    def name(self):
        return "tap_tester_adroll_start_date"

    def testable_streams(self):
        return set(self.expected_streams()).difference(
            { # STREAMS THAT CANNOT CURRENTLY BE TESTED
                'ad_reports', 'ads', 'ad_groups', 'segments', 'campaigns'
            }
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

        # Initialize start_date to make assertions
        self.START_DATE = self.get_properties().get('start_date')


        # get expected records
        expected_records = {x: [] for x in self.expected_streams()} # ids by stream
        for stream in self.testable_streams():
            existing_objects = self.client.get_all(stream)
            assert existing_objects, "Test data is not properly set for {}, test will fail.".format(stream)
            print("Data exists for stream: {}".format(stream))
            for obj in existing_objects:
                expected_records[stream].append({'eid': obj.get('eid')})

            # TODO if no objects exist within the 2nd start_date, create one
            # new_object = self.client.create_advertisable()               
            # expected_records[stream].append({'eid': obj.get('eid')})

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
        
        # Select all availabl streams and their fields
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=found_catalogs)

        catalogs = menagerie.get_catalogs(conn_id)

        #clear state
        menagerie.set_state(conn_id, {})

        ##########################################################################
        ### First Sync
        ##########################################################################

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

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        start_date_1 = self.get_properties()['start_date']
        self.START_DATE = dt.strftime(dt.strptime(self.START_DATE, self.START_DATE_FORMAT) \
                                      + timedelta(days=1), self.START_DATE_FORMAT)
        start_date_2 = self.START_DATE
        print("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(start_date_1, start_date_2))

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

        for stream in self.testable_streams():
            with self.subTest(stream=stream):
                replication_type = self.expected_replication_method().get(stream)

                # Testing how INCREMENTAL streams handle start date
                if replication_type == self.INCRMENTAL:
                    print("skipping incremental stream: {}".format(stream))
                    # TODO Verify 1st sync (start date=today-N days) record count > 2nd sync (start_date=today) record count.

                    # Verify that each stream has less records in 2nd sync than the 1st.
                    self.assertLess(record_count_by_stream_2.get(stream, 0),
                                    record_count_by_stream_1.get(stream,0),
                                    msg="Stream '{}' is {}\n".format(stream, self.FULL_TABLE) +
                                    "Expected sync with start date {} to have the less records".format(start_date_2) +
                                    "than sync with start date {}. It does not.".format(start_date_1))

                    # TODO Verify all data from later start data has bookmark values >= start_date.

                    # TODO Verify min bookmark sent to the target for 2nd sync >= start date.

                # Testing how FULL TABLE streams handle start date
                elif replication_type == self.FULL:
                    # TODO Verify that a bookmark doesn't exist for the stream.

                    # Verify that the 2nd sync includes the same number of records as the 1st sync.
                    # -> Currently full table does not obey start_date, which makes this assertion valid
                    self.assertEqual(record_count_by_stream_2.get(stream, 0),
                                     record_count_by_stream_1.get(stream,0),
                                     msg="Stream '{}' is {}\n".format(stream, self.FULL_TABLE) +
                                     "Expected sync with start date {} to have the same amount of records".format(start_date_2) +
                                     "than sync with start date {}. It does not.".format(start_date_1))
                    
                    # TODO Verify all records in the 2nd sync are included in the 1st sync since
                    # 2nd sync has a later start date.

                    print("skipping full table stream: {}".format(stream))

                else:
                    raise Exception("Expectations are set incorrectly. {} cannot have a "
                                    "replication method of {}".format(stream, replication_type))
                    
        # TODO Remove when test complete
        print("\n\n\tTOOD's PRESENT | The test is incomplete\n\n")


if __name__ == '__main__':
    unittest.main()

from time import sleep
import unittest
import simplejson

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestAdrollBase
from test_client import TestClient

class TestAdrollFullReplication(TestAdrollBase):
    def name(self):
        return "tap_tester_adroll_full_replication"

    def streams_creatable(self):
        """Streams which cannot currently have new records created in-test."""
        return self.expected_full_table_streams().difference({
            'advertisables', 'segments', 'ad_reports', 'campaigns'
        })


    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema)

    @classmethod
    def setUpClass(cls):
        print("\n\nTEST SETUP\n")
        cls.client = TestClient()


    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")

    def strip_format(self, date_value):
        try:
            date_stripped = dt.strptime(date_value, "%Y-%m-%dT%H:%M:%SZ")
            return date_stripped
        except ValueError:
            try:
                date_stripped = dt.strptime(date_value, "%Y-%m-%dT%H:%M:%S+0000Z")
                return date_stripped
            except ValueError:
                raise NotImplementedError

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
        # Ensure data exists prior to test for all full table streams
        expected_records_1 = {x: [] for x in self.expected_streams()}
        for stream in self.expected_full_table_streams():
            existing_objects = self.client.get_all(stream)
            assert existing_objects, "Test data is not properly set for {}, test will fail.".format(stream)
            print("Data exists for stream: {}".format(stream))
            for obj in existing_objects:
                expected_records_1[stream].append(obj)
        # TODO Create 1 new record for every full table stream that is missing data

        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        full_streams = {key for key, value in self.expected_replication_method().items()
                        if value == self.FULL}
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in full_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), full_streams,
                         msg="Expect first_sync_record_count keys {} to equal full_streams {},"
                         " first_sync_record_count was {}".format(
                             first_sync_record_count.keys(),
                             full_streams,
                             first_sync_record_count))

        first_sync_state = menagerie.get_state(conn_id)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        # Create 1 new record for every full table stream
        N = 1  # number of creates/updates between syncs
        expected_records_2 = {x: [] for x in self.expected_streams()}
        for stream in self.streams_creatable():
            for _ in range(N):
                print("CREATING A RECORD FOR STREAM: {}".format(stream))
                new_object = self.client.create(stream)
                expected_records_2[stream].append(new_object)

        # Update 1 existing record for every full table stream
        for stream in ['ads']: # self.streams_creatable(): # TODO
            for _ in range(N):
                print("UDPATING A RECORD FOR STREAM: {}".format(stream))
                # eid = expected_records_1.get(stream)[-1] # most recent record prior to test
                updated_object = self.client.update(stream).get('original') #, eid=eid)
                expected_records_2[stream].append(updated_object)

        # adjust expectations to include expected_records_1
        for stream in self.streams_creatable():
            for record in expected_records_1.get(stream):
                if record.get('eid') in [ex_rec.get('eid') for ex_rec in expected_records_2.get(stream, [])]:
                    continue  # don't add a record to expectations twice
                expected_records_2[stream].append(record)
                # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        # Loop first_sync_records and compare against second_sync_records
        # each iteration of loop is chekcing both for a stream
        # first_sync_records["ads"] == second["ads"]
        for stream in full_streams:
            with self.subTest(stream=stream):
                # RECORD COUNT
                record_count_1 = first_sync_record_count.get(stream, 0)
                record_count_2 = second_sync_record_count.get(stream, 0)
                # ACTUAL RECORDS
                records_from_sync_1 = set(row.get('data').get('eid')
                                          for row in first_sync_records.get(stream, []).get('messages', []))
                records_from_sync_2 = set(row.get('data').get('eid')
                                          for row in second_sync_records.get(stream, []).get('messages', []))
                # EXPECTED_RECORDS
                expected_records_from_sync_1 = set(record.get('eid') for record in expected_records_1.get(stream, []))
                expected_records_from_sync_2 = set(record.get('eid') for record in expected_records_2.get(stream, []))
                
                # verify there is no bookmark values from state
                state_value = first_sync_state.get("bookmarks", {}).get(stream)
                self.assertIsNone(state_value)

                # verify that there is more than 1 record of data - setup necessary
                self.assertGreater(record_count_1, 1, msg="Data isn't set up to be able to test full sync")

                # verify that you get the same or more data the 2nd time around
                self.assertGreaterEqual(record_count_2, record_count_1,
                                        msg="second syc didn't have more records, full sync not verified")

                # verify all expected records were replicated for first sync
                self.assertEqual(
                        set(), records_from_sync_1.symmetric_difference(expected_records_from_sync_1),
                        msg="1st Sync records do not match expectations.\n" +
                        "MISSING RECORDS: {}\n".format(expected_records_from_sync_1.symmetric_difference(records_from_sync_1)) +
                        "ADDITIONAL RECORDS: {}".format(records_from_sync_1.symmetric_difference(expected_records_from_sync_1))
                )

                # verify all data from 1st sync included in 2nd sync
                self.assertEqual(set(), records_from_sync_1.difference(records_from_sync_2),
                                 msg="Data in 1st sync missing from 2nd sync")

                # testing streams with new and updated data
                if stream in self.streams_creatable():

                    # verify that the record count has increased by N record in the 2nd sync, where
                    # N = the number of new records created between sync 1 and sync 2
                    self.assertEqual(record_count_2, record_count_1 + N,
                                     msg="Expected {} new records to be captured by the 2nd sync.\n".format(N) +
                                     "Record Count 1: {}\nRecord Count 2: {}".format(record_count_1, record_count_2)
                    )

                    # verify that the newly created and updated records are captured by the 2nd sync
                    self.assertEqual(
                        set(), records_from_sync_2.symmetric_difference(expected_records_from_sync_2),
                        msg="2nd Sync records do not match expectations.\n" +
                        "MISSING RECORDS: {}\n".format(expected_records_from_sync_2.symmetric_difference(records_from_sync_2)) +
                        "ADDITIONAL RECORDS: {}".format(records_from_sync_2.symmetric_difference(expected_records_from_sync_2))
                    )

                    # verify that the updated records are correctly captured by the 2nd sync
                    expected_updated_records = set(record.get('eid') for record in expected_records_2.get(stream, [])
                                                   if "UPDATED" in record.get('name'))
                    if expected_updated_records:
                        updated_records_from_sync_2 = set(row.get('data').get('eid')
                                                          for row in second_sync_records.get(stream, []).get('messages', [])
                                                          if "UPDATED" in row.get('data').get('name'))
                        self.assertEqual(
                            set(), updated_records_from_sync_2.symmetric_difference(expected_updated_records),
                            msg="Failed to replicate the updated {} record(s)\n".format(stream) +
                            "MISSING RECORDS: {}\n".format(expected_updated_records.difference(updated_records_from_sync_2)) +
                            "ADDITIONAL RECORDS: {}\n".format(updated_records_from_sync_2.difference(expected_updated_records))
                        )

        # TODO Remove when test complete
        print("\n\n\tTOOD's PRESENT | The test is incomplete\n\n")


if __name__ == '__main__':
    unittest.main()

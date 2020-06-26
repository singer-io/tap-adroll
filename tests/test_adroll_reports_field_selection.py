import os
import unittest
from decimal import Decimal
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce

from singer import metadata
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestAdrollBase
from test_client import TestClient


class TestAdrollReportsFieldSelection(TestAdrollBase):
    """
    Test that with all fields selected for the 'ad_reports' stream  we replicate data as expected.
    Test that with only automatic fields selected for the 'ad_reports' stream  we replicate data as expected.
    """

    def name(self):
        return "tap_tester_adroll_reports_fields"

    def testable_streams(self):
        return self.expected_incremental_streams()

    @classmethod
    def setUpClass(cls):
        print("\n\nTEST SETUP\n")
        cls.client = TestClient()

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")

    def expected_automatic_fields(self):
        fks = self.expected_foreign_keys()
        pks = self.expected_primary_keys()

        return {stream: fks.get(stream, set()) | pks.get(stream, set())
                for stream in self.expected_streams()}

    def format_expected_data(self, records_by_stream):
        """Type the data to match expected tap output"""
        for stream, stream_records in records_by_stream.items():
            print("Altering records for {} ".format(stream))
            for record in stream_records:
                for key, value in record.items():
                    if type(value) == float:
                        record[key] = Decimal(str(value))

    def test_run(self):
        """
        Verify that for each stream you can get data when no fields are selected
        and only the automatic fields are replicated.
        """

        print("\n\nRUNNING {}\n\n".format(self.name()))

        # Initialize start_date state to capture ad_reports records
        self.START_DATE = self.timedelta_formatted(self.REPORTS_START_DATE, -1)
        self.END_DATE = self.REPORTS_END_DATE
        print("INCREMENTAL STREAMS RELY ON A STATIC DATA SET. SO WE TEST WITH:\n" +
              "  START DATE 1 | {}\n".format(self.START_DATE) +
              "  END DATE 2 | {}".format(self.END_DATE))

        # ensure data exists for sync streams and set expectations
        expected_records_all = {x: [] for x in self.expected_streams()} # all fields selected
        expected_records_auto = {x: [] for x in self.expected_streams()} # no fields selected
        for stream in self.testable_streams():
            start_date = self.parse_date(self.START_DATE)
            end_date = self.parse_date(self.END_DATE)
            existing_objects = self.client.get_all(stream, start_date, end_date)

            assert existing_objects, "Test data is not properly set for {}, test will fail.".format(stream)
            print("Data exists for stream: {}".format(stream))
            for obj in existing_objects:
                expected_records_all[stream].append(obj)
                expected_records_auto[stream].append(
                    {field: obj.get(field)
                     for field in self.expected_automatic_fields().get(stream)}
                )

        # format expected data to match expected output of tap
        self.format_expected_data(expected_records_all)

        # Instantiate connection with default start/end dates
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

        ##########################################################################
        ### ALL FIELDS SYNC
        ##########################################################################

        # Select all available fields from all streams
        exclude_streams = self.expected_streams().difference(self.testable_streams())
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=found_catalogs, select_all_fields=True, exclude_streams=exclude_streams
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection worked
        for cat in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])
            # Verify only testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if not cat['stream_name'] in self.testable_streams():
                # None expected for {'inclusion':'available'} happens when menagerie "deselects" stream
                self.assertTrue(selected is None, msg="Stream is selected, but shouldn't be.")
                continue 
            self.assertTrue(selected, msg="Stream not selected.")

            # Verify all fields within each selected stream are selected
            for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                field_selected = field_props.get('selected')
                print("\tValidating selection on {}.{}: {}".format(cat['stream_name'], field, field_selected))
                self.assertTrue(field_selected, msg="Field not selected.")

        #clear state
        menagerie.set_state(conn_id, {})

        # run sync with all fields selected
        sync_job_name_all = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status_all = menagerie.get_exit_status(conn_id, sync_job_name_all)
        menagerie.verify_sync_exit_status(self, exit_status_all, sync_job_name_all)

        # read target output
        record_count_by_stream_all = runner.examine_target_output_file(self, conn_id,
                                                                       self.expected_streams(),
                                                                       self.expected_primary_keys())
        replicated_row_count_all =  reduce(lambda accum,c : accum + c, record_count_by_stream_all.values())
        synced_records_all = runner.get_records_from_target_output()

        # Verify target has records for all synced streams
        for stream, count in record_count_by_stream_all.items():
            assert stream in self.expected_streams()
            if stream in self.testable_streams():
                self.assertGreater(count, 0, msg="failed to replicate any data for: {}".format(stream))
        print("total replicated row count: {}".format(replicated_row_count_all))

        ##########################################################################
        ### AUTOMATIC FIELDS SYNC
        ##########################################################################

        # Instantiate connection with default start/end dates
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

        # Select no available fields (only automatic) for  all testable streams
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=found_catalogs, select_all_fields=False, exclude_streams=exclude_streams
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection worked
        for cat in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])
            # Verify all streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if not cat['stream_name'] in self.testable_streams():
                self.assertTrue(selected is None, msg="Stream is selected, but shouldn't be.")
                continue 
            self.assertTrue(selected, msg="Stream not selected.")

            # Verify only automatic fields are selected
            for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                field_selected = field_props.get('selected')
                print("\tValidating selection on {}.{}: {}".format(cat['stream_name'], field, field_selected))

                if field in self.expected_automatic_fields().get(cat['stream_name']):
                    # NOTE: AUTOMATIC FIELDS IGNORE THE SELECTED md {'selected': None}
                    print("NOTE: selection for {} is ignored by the Transformer ".format(field) +
                          " so long as 'inlcusion' = 'automatic'")
                else:
                    self.assertFalse(field_selected, msg="Field is selected but not automatic.")

        # run sync with no fields selected (only automatic)
        sync_job_name_auto = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status_auto = menagerie.get_exit_status(conn_id, sync_job_name_auto)
        menagerie.verify_sync_exit_status(self, exit_status_auto, sync_job_name_auto)

        # read target output
        record_count_by_stream_auto = runner.examine_target_output_file(self, conn_id,
                                                                       self.expected_streams(),
                                                                       self.expected_primary_keys())
        replicated_row_count_auto =  reduce(lambda accum,c : accum + c, record_count_by_stream_auto.values())
        synced_records_auto = runner.get_records_from_target_output()

        # Verify target has records for all synced streams
        for stream, count in record_count_by_stream_auto.items():
            assert stream in self.expected_streams()
            if stream in self.testable_streams():
                self.assertGreater(count, 0, msg="failed to replicate any data for: {}".format(stream))
        print("total replicated row count: {}".format(replicated_row_count_auto))

        # Test by Stream
        for stream in self.testable_streams():
            with self.subTest(stream=stream):
                ##########################################################################
                ### TESTING ALL FIELDS
                ##########################################################################

                data = synced_records_all.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                expected_keys = expected_records_all.get(stream)[0].keys()

                # Verify schema covers all fields
                schema_keys = set(self.expected_schema_keys(stream))
                self.assertEqual(
                    set(), set(expected_keys).difference(schema_keys),
                    msg="\nFields missing from schema: {}\n".format(set(expected_keys).difference(schema_keys))
                )

                # not a test, just logging the fields that are included in the schema but not in the expectations
                if schema_keys.difference(set(expected_keys)):
                    print("WARNING Fields missing from expectations: {}".format(schema_keys.difference(set(expected_keys))))

                # Verify that all fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertEqual(
                        actual_keys.symmetric_difference(schema_keys), set(),
                        msg="Expected all fields, as defined by schemas/{}.json".format(stream)
                    )

                actual_records = [row['data'] for row in data['messages']]
                expected_records = expected_records_all.get(stream)

                # NOTES ####################################################################################################
                # TODO | Verify those 'eid' values are for ads.
                ############################################################################################################

                # Verify the number of records match expectations
                self.assertEqual(len(expected_records),
                                 len(actual_records),
                                 msg="Number of actual records do match expectations. " +\
                                 "Check expectations, check for duplicate records in Target.")

                # verify there are no dup records in the target
                already_tracked = []
                for actual_record in actual_records:
                    if actual_record in already_tracked:
                        continue
                    already_tracked.append(actual_record)
                self.assertEqual(len(already_tracked), len(actual_records), msg="DUPLICATES PRESENT")

                # verify by values, that we replicated the expected records
                for actual_record in actual_records:
                    self.assertTrue(actual_record in expected_records,
                                    msg="Actual record missing from expectations\n" +
                                    "Actual Record: {}".format(actual_record))
                for expected_record in expected_records:
                    self.assertTrue(expected_record in actual_records,
                                    msg="Expected record missing from target." +
                                    "Expected Record: {}".format(expected_record))

                ##########################################################################
                ### TESTING AUTOMATIC FIELDS
                ##########################################################################

                data = synced_records_auto.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                expected_keys = self.expected_automatic_fields().get(stream)

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertEqual(
                        actual_keys.symmetric_difference(expected_keys), set(),
                        msg="Expected automatic fields and nothing else.")

                actual_records = [row['data'] for row in data['messages']]
                expected_records = expected_records_auto.get(stream)

                #Verify the number of records match expectations
                self.assertEqual(len(expected_records),
                                 len(actual_records),
                                 msg="Number of actual records do match expectations. " +\
                                 "We probably have duplicate records.")

                # verify there are no dup records in the target
                already_tracked = []
                for actual_record in actual_records:
                    if actual_record in already_tracked:
                        continue
                    already_tracked.append(actual_record)
                self.assertEqual(len(already_tracked), len(actual_records), msg="DUPLICATES PRESENT")

                # verify by values, that we replicated the expected records
                for actual_record in actual_records:
                    self.assertTrue(actual_record in expected_records,
                                    msg="Actual record missing from expectations\n" +
                                    "Actual Record: {}".format(actual_record))
                for expected_record in expected_records:
                    self.assertTrue(expected_record in actual_records,
                                    msg="Expected record missing from target." +
                                    "Expected Record: {}".format(expected_record))

        # TODO Remove when test complete
        print("\n\n\tTOOD's PRESENT | The test is incomplete\n\n")


if __name__ == '__main__':
    unittest.main()

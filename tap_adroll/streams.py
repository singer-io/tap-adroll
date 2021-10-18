import datetime

from singer import utils
import singer

LOGGER = singer.get_logger()


class Stream:
    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


class Advertisables(Stream):
    stream_id = 'advertisables'
    stream_name = 'advertisables'
    endpoint = 'organization/get_advertisables'
    key_properties = ["eid"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    advertisable_eids = []

    def get_all_advertisable_eids(self):
        if Advertisables.advertisable_eids:
            for eid in Advertisables.advertisable_eids:
                yield eid
        else:
            records = self.client.get(self.endpoint)
            for rec in records.get('results'):
                Advertisables.advertisable_eids.append(rec['eid'])
                yield rec['eid']


    def sync(self):
        records = self.client.get(self.endpoint)
        for rec in records.get('results'):
            yield rec


class Ads(Stream):
    stream_id = 'ads'
    stream_name = 'ads'
    endpoint = 'advertisable/get_ads'
    key_properties = ["eid"]
    replication_method = "FULL_TABLE"
    replication_keys = []


    def sync(self):
        advertisables = Advertisables(self.client, self.config, self.state)
        for advertisable_eid in advertisables.get_all_advertisable_eids():
            records = self.client.get(self.endpoint, params={
                'advertisable': advertisable_eid
            })
            for rec in records.get('results'):
                yield rec


class AdReports(Stream):
    stream_id = 'ad_reports'
    stream_name = 'ad_reports'
    endpoint = 'report/ad'
    key_properties = ["eid", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["date"]


    def generate_daily_date_windows(self):
        bookmark = singer.bookmarks.get_bookmark(self.state, self.stream_name, self.replication_keys[0])
        lookback_window = datetime.timedelta(days=int(self.config.get('lookback_window') or 7))
        report_date = min(utils.strptime_to_utc(bookmark or self.config['start_date']),
                          utils.now() - lookback_window)

        end_date = utils.now()
        if self.config.get('end_date'):
            end_date = utils.strptime_to_utc(self.config.get('end_date'))

        if report_date > end_date:
            LOGGER.warning("Calculated report_date %s is greater than end_date %s; no reports will be retrieved.",
                           datetime.datetime.strftime(report_date, "%Y-%m-%d"),
                           datetime.datetime.strftime(end_date, "%Y-%m-%d")
                           )

        while report_date <= end_date:
            yield report_date
            report_date += datetime.timedelta(days=1)

    def sync(self):
        advertisables = Advertisables(self.client, self.config, self.state)
        # Daily Pagination writes a bookmark after each day
        for report_date in self.generate_daily_date_windows():
            request_date = datetime.datetime.strftime(report_date, "%m-%d-%Y")
            for advertisable_eid in advertisables.get_all_advertisable_eids():
                LOGGER.info("Syncing %s for advertisable %s for date %s", self.stream_id,
                            advertisable_eid, report_date)
                records = self.client.get(self.endpoint, params={
                    'advertisable': advertisable_eid,
                    'data_format': 'entity',
                    'start_date': request_date,
                    'end_date': request_date,
                })
                for rec in records.get('results'):
                    rec['date'] = datetime.datetime.strftime(report_date, "%Y-%m-%dT00:00:00.000000Z")
                    yield rec

            # Write bookmark after syncing all Advertisables for the day
            singer.bookmarks.write_bookmark(self.state, self.stream_name, self.replication_keys[0], utils.strftime(report_date))
            singer.write_state(self.state)


class Segments(Stream):
    #advertisable/get_segments
    stream_id = 'segments'
    stream_name = 'segments'
    endpoint = 'advertisable/get_segments'
    key_properties = ["eid"]
    # It seems like this endpoint now has pagination and filtering capabilities.
    replication_method = "FULL_TABLE"
    replication_keys = []


    def sync(self):
        advertisables = Advertisables(self.client, self.config, self.state)
        for advertisable_eid in advertisables.get_all_advertisable_eids():
            records = self.client.get(self.endpoint, params={
                'advertisable': advertisable_eid
            })
            for rec in records.get('results'):
                yield rec

class Campaigns(Stream):
    stream_id = 'campaigns'
    stream_name = 'campaigns'
    endpoint = 'advertisable/get_campaigns'
    key_properties = ["eid"]
    # It seems like this endpoint now has pagination and filtering capabilities.
    replication_method = "FULL_TABLE"
    replication_keys = []


    def sync(self):
        # TODO: Can switch on `is_active` by default "True" returning only active campaigns
        advertisables = Advertisables(self.client, self.config, self.state)
        for advertisable_eid in advertisables.get_all_advertisable_eids():
            records = self.client.get(self.endpoint, params={
                'advertisable': advertisable_eid
            })
            for rec in records.get('results'):
                yield rec


class AdGroups(Stream):
    stream_id = 'ad_groups'
    stream_name = 'ad_groups'
    endpoint = 'advertisable/get_adgroups'
    key_properties = ["eid"]
    # It seems like this endpoint now has pagination and filtering capabilities.
    replication_method = "FULL_TABLE"
    replication_keys = []


    def sync(self):
        # TODO: Can switch on `camp_active` by default "True" returning only for active campaigns
        advertisables = Advertisables(self.client, self.config, self.state)
        for advertisable_eid in advertisables.get_all_advertisable_eids():
            records = self.client.get(self.endpoint, params={
                'advertisable': advertisable_eid
            })
            for rec in records.get('results'):
                yield rec


STREAM_OBJECTS = {
    'advertisables': Advertisables,
    'ads': Ads,
    'ad_groups': AdGroups,
    'ad_reports': AdReports,
    'segments': Segments,
    'campaigns': Campaigns,
}

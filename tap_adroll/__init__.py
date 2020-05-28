import singer
from singer import utils
from singer.catalog import Catalog, write_catalog
from tap_adroll.discover import do_discover
from tap_adroll.client import AdrollClient

LOGGER = singer.get_logger()


def do_sync(client, config, state, catalog):
    pass


@utils.handle_top_exception(LOGGER)
def main():
    #required_config_keys = ['start_date']
    required_config_keys = []
    args = singer.parse_args(required_config_keys)

    config = args.config
    client = AdrollClient(args.config_path, config)
    catalog = args.catalog or Catalog([])
    state = args.state

    if args.properties and not args.catalog:
        raise Exception("DEPRECATED: Use of the 'properties' parameter is not supported. Please use --catalog instead")

    if args.discover:
        LOGGER.info("Starting discovery mode")
        catalog = do_discover()
        write_catalog(catalog)
    else:
        LOGGER.info("do_sync")
        #do_sync(client, config, state, catalog)

if __name__ == "__main__":
    main()

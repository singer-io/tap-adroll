import singer
from singer import utils
from singer.catalog import Catalog, write_catalog

LOGGER = singer.get_logger()

def do_discover():
    pass

def do_sync(client, config, state, catalog):
    pass

def write_catalog(catalog):
    pass

@utils.handle_top_exception(LOGGER)
def main():
    #required_config_keys = ['start_date']
    required_config_keys = []
    args = singer.parse_args(required_config_keys)

    config = args.config  # pylint:disable=unused-variable
    #client = TrelloClient(config)  # pylint:disable=unused-variable
    catalog = args.catalog or Catalog([])
    state = args.state # pylint:disable=unused-variable

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

import argparse
import singer
from singer import utils
from singer.catalog import Catalog, write_catalog
from tap_adroll.discover import do_discover
from tap_adroll.client import AdrollClient
from tap_adroll.sync import do_sync

LOGGER = singer.get_logger()

def parse_args(required_config_keys):
    '''Parse standard command-line args.

    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:

    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
    -dev, --dev     Runs the tap in dev mode

    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '-s', '--state',
        help='State file')

    parser.add_argument(
        '-p', '--properties',
        help='Property selections: DEPRECATED, Please use --catalog instead')

    parser.add_argument(
        '--catalog',
        help='Catalog file')

    parser.add_argument(
        '-d', '--discover',
        action='store_true',
        help='Do schema discovery')

    parser.add_argument(
        '-dev', '--dev',
        action='store_true',
        help='Runs tap in dev mode')

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = utils.load_json(args.config)
    if args.state:
        setattr(args, 'state_path', args.state)
        args.state = utils.load_json(args.state)
    else:
        args.state = {}
    if args.properties:
        setattr(args, 'properties_path', args.properties)
        args.properties = utils.load_json(args.properties)
    if args.catalog:
        setattr(args, 'catalog_path', args.catalog)
        args.catalog = Catalog.load(args.catalog)

    utils.check_config(args.config, required_config_keys)

    return args

@utils.handle_top_exception(LOGGER)
def main():
    required_config_keys = ['start_date']
    args = parse_args(required_config_keys)
    config = args.config
    LOGGER.info("............................:%s",args.dev)
    if args.dev:
        LOGGER.info("11111111111111")
        LOGGER.warning("Executing Tap in Dev mode",) 
    client = AdrollClient(args.config_path, config, args.dev)

    catalog = args.catalog or Catalog([])
    state = args.state

    if args.properties and not args.catalog:
        raise Exception("DEPRECATED: Use of the 'properties' parameter is not supported. Please use --catalog instead")

    if args.discover:
        LOGGER.info("Starting discovery mode")
        catalog = do_discover()
        write_catalog(catalog)
    else:
        LOGGER.info("Starting sync mode")
        do_sync(client, config, state, catalog)

if __name__ == "__main__":
    main()

import sys
import os
import argparse
import time

from helper.functions import load_yaml
from helper.functions import check_file_path, check_dir_path, read_input_file, write_records_to_file
from gnfg_engine import initiate_gnfg as GNFG

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='gnfg_local_runner',
        usage='./%(prog)s.py -i input_file_path -o output_file_path -y yaml_file -v boolean',
        description='Run GNFG locally without having to use Kafka.'
    )

    parser.add_argument(
        '-v', '--verbose', help='Verbosity of the program.',
        type=bool, default=False,
    )

    parser.add_argument(
        '-o', '--output_file_path', help='Path of file to write the output.',
        type=str, required=False, default=None
    )

    parser.add_argument(
        '-y', '--yaml', help='Config file in yaml format',
        type=str, required=True
    )

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args = parser.parse_args()

    config = load_yaml(args.yaml)
    # VERBOSE = config['aca_params']['VERBOSE']
    VERBOSE = args.verbose

    output_file = args.output_file_path

    # create the graph driver
    graph_config = config['graph']
    paths = GNFG.start(graph_config=graph_config, VERBOSE=VERBOSE, output_file=output_file,)

    if VERBOSE:
        print(f"{time.ctime()} Processing complete!\n")

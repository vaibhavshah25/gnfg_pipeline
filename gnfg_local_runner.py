import sys
import os
import argparse
import time

from helper.functions import load_yaml
from helper.functions import check_file_path, check_dir_path, read_input_file, write_records_to_file

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
        '-i', '--input_file_path', help='Location to the CSV file containing the input.',
        type=str, required=True
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

    app_filepath = args.input_file_path
    check_file_path(app_filepath)

    output_file = args.output_file_path
    # check_file_path(output_file)

    # read the applications
    _, new_data = read_input_file(app_filepath)

    # Create neo4j driver
    if VERBOSE:
        print(f"Total applications to process: {len(new_data)}")
        print(f"{time.ctime()} Started path detection process")
        print(new_data)

    # create the graph driver
    graph_config = config['graph']
    paths = simulate_get_GNFG_paths(new_data, graph_config, VERBOSE)

    if VERBOSE:
        print(f"{time.ctime()} Processing complete!\n")

    if output_file:
        write_records_to_file(output_file, paths)
    else:
        print(paths)

"""Generalized, Automatic Data Dictionary Generator
author: Michael Giansiracusa
email: giansiracumt@ornl.gov

This module will use some raw (00 level) data files from a datastream, modify them, ingest them,
and compare them to the original to find out which variables are in which columns and automatically
generate a json style data dictionary.

Example: None yet

Attributes:
    DEBUG (bool): True to print debug messages
    DQR_REGEX (raw str): regex to search for dqr # in cwd path string
    DATASTREAM_REGEX (raw str): regex to search for datastream name in cwd path string
    DATE_REGEX (raw str): regex to search for a date in an input file name
    LINK_REGEX (raw str): regex to search for the ncreview link in the ncreview log file
    HELP_DESCRIPTION (str): help description for argparse
    EXAMPLE (str): example for argparse
"""

import argparse
import os
import shutil
import subprocess
import csv
import re
from glob import glob
# import netCDF4

DEBUG = True

DQR_REGEX = re.compile(r"D\d{6}(\.)*(\d)*")
DATASTREAM_REGEX = re.compile(r"(acx|awr|dmf|fkb|gec|hfe|mag|mar|mlo|nic|nsa|osc|pgh|pye|"
                              r"sbs|shb|tmp|wbu|zrh|asi|cjc|ena|gan|grw|isp|mao|mcq|nac|"
                              r"nim|oli|osi|pvc|rld|sgp|smt|twp|yeu)\w+\.(\w){2}")
DATE_REGEX = re.compile(r"[1,2]\d{7}")
LINK_REGEX = re.compile(r"https:([^\\]*)")

HELP_DESCRIPTION = '''
This program will automate testing the variable mapping from raw to cdf/nc files.
It must be run from the directory where the input raw files are.
'''

EXAMPLE = '''
EXAMPLE: TODO
'''


def parse_args():
    """Return a Namespace with command line arguments and default values."""

    parser = argparse.ArgumentParser(description=HELP_DESCRIPTION, epilog=EXAMPLE,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input', dest='input', type=str, default=os.getcwd(),
                        help='input directory, ex. /data/project/12345/D111042/collection/sgp/sgpmetE13.00')
    parser.add_argument('-m', '--modify', dest='modify', type=int, default=0,
                        help='Column to modify and test. Zero is the first column.')
    parser.add_argument('-n', '--mod_num', dest="mod_num", type=float, default=100,
                        help="Number to add to specified column.")
    parser.add_argument('-c', '--comp_dir', dest='comp_dir', type=str, default=None,
                        help='Directory with files to ncreview compare output against.')
    parser.add_argument('--no-cleanup', dest="no_clean", action="store_true",
                        help="when true, does not cleanup datastream directory.")
    parser.add_argument('--create-dict', dest='create_dict', action="store_true")
    parser.add_argument('--delimiter', dest='delimiter', default=',', help='File delimiter.')
    parser.add_argument('--header', dest='header', type=int, default=0,
                        help='Number of header lines. Rows skipped when modifying columns')
    parser.add_argument('--skip-col', dest='skip_column', nargs='+', type=int, default=[],
                        help='Columns to skip. Must be last argument.')
    parser.add_argument('-I', '--Interactive', action='store_true', dest='interactive',
                        default=False, help='Interactive / partial execution')

    args = parser.parse_args()
    if args.modify in args.skip_column:
        print('Skipping modification column, no effect.')
        exit(0)
    if args.create_dict:
        if not args.header:
            args.header = input("Number of header lines = ")
        if not args.skip_column:
            num = 0
            while True:
                user_input = input("Column to skip? (done to finish) int = ")
                if user_input == "done":
                    break
                try:
                    args.skip_column.append = int(user_input)
                except ValueError:
                    if num >= 10:
                        print("Too many errors, exiting.")
                        exit(1)
                    else:
                        print("Column number must be an int, first column is 0.")
                        num += 1
    return args


def parse_datastream(datastream: str) -> [str, str, str]:
    """Split a datastream into the site instrument and facility components.

    :param datastream: string representation of the name of the datastream.
    :return: list containing the components that make up a datastream name.
    """
    site = datastream[:3]
    instrument = datastream[3:-2]
    facility = datastream[-2:]
    return site, instrument, facility


def reproc_env(dqr: str) -> [str, str, str]:
    """Get the environment varieables for this reprocessing job.

    :param dqr: string representing the dqr number of this job.
    :return: list of the relevant environment variables for this job.
    """
    reproc_home = os.getenv("REPROC_HOME")  # expected to be the current reproc environment
    post_processing = os.getenv("POST_PROC")  # is a post processing folder under reproc home
    data_home = f"{reproc_home}/{dqr}"  # used to set environment variables for current dqr job
    return reproc_home, post_processing, data_home


def setup_environment(dqr: str) -> None:
    """Setup and source the environment variables for this reprocessing job.

    :param dqr: string representing the dqr number of this job.
    :return: None
    """
    _, _, data_home = reproc_env(dqr)  # get reproc environment variables

    # set environment variables based on this default dictionary
    env_vars = {"DATA_HOME": data_home,  # usually dqr folder under reproc env folder
                "DATASTREAM_DATA": f"{data_home}/datastream",
                "ARCHIVE_DATA": "/data/archive",
                "OUT_DATA": f"{data_home}/out",
                "TMP_DATA": f"{data_home}/tmp",
                "HEALTH_DATA": f"{data_home}/health",
                "QUICKLOOK_DATA": f"{data_home}/quicklooks",
                "COLLECTION_DATA": f"{data_home}/collection",
                "CONF_DATA": f"{data_home}/conf",
                "LOGS_DATA": f"{data_home}/logs",
                "WWW_DATA": f"{data_home}/www",
                "DB_DATA": f"{data_home}/db"}

    print("\nSourcing from default dict:")
    for key, value in env_vars.items():
        os.environ[key] = value
        print("\t{} = {}".format(key, os.environ[key]))


def get_files(search_arg: str) -> list:
    """Wraps glob for readability in workflow, returns file list.

    :param search_arg: regular expression for file search.
    :return: list of strings, full paths to original input files.
    """
    return glob(search_arg)


def backup_input_files(input_dir: str, files: list) -> None:
    """Moves files from input directory to a backup directory.
    This is necessary because the ingest will rename and move the input files automatically.

    :param input_dir: the directory of the original input files
    :param files: list of strings, full paths to original input files.
    :return: None
    """
    print("Coping files into .autotest dir...")
    autotest_dir = os.path.join(input_dir, ".autotest")
    if not os.path.exists(autotest_dir):
        os.makedirs(autotest_dir)
        print('Created directory: {}'.format(autotest_dir))
    mod_files = []
    for file_name in files:
        src = file_name
        dest = os.path.join(autotest_dir, os.path.basename(file_name))
        try:
            if not os.path.exists(dest):
                shutil.copy(src, dest)
                print("\tMoved: {}".format(src))
            else:
                print("\tExits: {}".format(dest))
            mod_files.append(dest)
        except shutil.SameFileError:
            print("\tExits: {}".format(dest))
    print("Finished copying files.\n")


def cleanup_postproc(dqr: str) -> None:
    """Remove old ncreview comparison directory and log files.
    If this is not done, the ncreview wrapper script will make a new comparison directory and
    append to the log file.

    :param dqr: list of strings, full paths to original input files.
    :return: None
    """
    _, post_processing, _ = reproc_env(dqr)
    print("Cleaning up old ncreview files")
    rm_path = os.path.join(post_processing, dqr, 'ncr*')
    os.system("rm -rvf {}".format(rm_path))
    print("Finished cleanup.\n")


def modify_files(args: argparse.Namespace, files: list) -> None:
    """Modify one column in all the raw, ascii, csv input files.
    The modification is used to compare the old cdf and new cdf
    files and determine which variable is in which column.

    :param args: an argparse.Namespace variable with command line arguments and defaults
    :param files: list of strings, full paths to original input files.
    :return: None
    """
    print("Modifying files... ")
    for input_file in files:
        output_list = []
        with open(input_file) as open_input_file:
            csv_reader = csv.reader(open_input_file, delimiter=args.delimiter)
            for j, line in enumerate(csv_reader):
                if j >= args.header:
                    try:
                        try:
                            num = int(line[args.modify])
                        except ValueError:
                            num = float(line[args.modify])
                        line[args.modify] = num + args.mod_num
                    except IndexError:
                        pass
                    output_list.append(line)
                else:
                    output_list.append(line)
        output_file = input_file.split("/")[-1]
        with open(output_file, 'w') as open_output_file:
            csv_writer = csv.writer(open_output_file)
            csv_writer.writerows(output_list)
        print("modified {}".format(output_file))
    print("Finished modifying files.\n")


def get_ingest_command(site: str, datastream: str) -> str:
    """Get the command used to create one of the next level.
    This will be used to ingest the modified raw files.

    :param site: ARM site that the datastream came from.
    :param datastream: name of the datasteram.
    :param result_date: date to look form specific file in next level data.
    :return: the shell command used to create the next level data from the input data.
    """
    ingest_search = os.path.join("/data/archive/", site, datastream[:-3] + "*")
    print("Searching for output directories:\n\t{}".format(ingest_search))
    cmd = 'ls -d {}'.format(ingest_search)
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    out, _ = proc.communicate()
    out_2string_striped = str(out)[2:-3]
    out_split = out_2string_striped.split('\\n')
    print("Found the following directories:\n\t{}".format(out_split))
    for element in out_split:
        if element[-2:] != '00':
            search_dir = os.path.join("/data/archive", site, element, "*")
            print('Searching for netcdf file:\n\t{}'.format(search_dir))
            ingested_file = glob(search_dir)[-1]
            cmd = "ncdump -h {} | grep command".format(ingested_file)
            print(cmd)
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            out, _ = proc.communicate()
            ingest_command = str(out).split('"')[1]
            if ingest_command:
                print("Ingest command: {}\n".format(ingest_command))
                return ingest_command
    return ""


def ingest_files(site: str, datastream: str) -> None:
    """Run the appropriate ingest to get the next level data from the one in the input directory
    This method gets the date off one input file, uses it to search /data/archive for an output
    file, uses ncdump and grep to get the command used to create that output file, then runs
    that command on the files in the input directory. A specific input and output path are
    required for ingest to run. This structure can be created automatically with ARM Reprocessing
    Manager module.

    :param files: list of strings, full paths to original input files.
    :param site: ARM site that the datastream came from.
    :param datastream:
    :return:
    """
    # get ingest command
    ingest_command = get_ingest_command(site, datastream)
    if ingest_command is None:
        print("Error: not enough info to run ingest."
              "\n\tingest_command = {}".format(ingest_command))
        exit(1)
    print("Running ingest command: <{}> ... ".format(ingest_command), end="")
    proc = subprocess.Popen(ingest_command, shell=True, stdout=subprocess.PIPE)
    _, err = proc.communicate()
    print("Finished running ingest.\n\tErrors: {}\n".format(err))


def ncreview_setup(dqr: str, site: str, comp_dir: str) -> tuple:
    """Construct the command to run the ncreview wrapper
    The wrapper must be run in the output cdf directory, this method will return a list
    of output cdf directories and the command to run the wrapper, both as elements of a tuple.

    :param dqr: string representing the dqr number of this job.
    :param site: ARM site that the datastream came from.
    :return: tuple(list(str), str)
    """
    # get reprocessing environment variables
    reproc_home, _, _ = reproc_env(dqr)
    print("Setting up for ncreveiw... ")
    if comp_dir != None:
        ncr_cmd = "python3.6 /data/home/giansiracusa/reprocessing_scripts/ncr_cmd.py -c {}".format(comp_dir)
    else:
        ncr_cmd = "python3.6 /data/home/giansiracusa/reprocessing_scripts/ncr_cmd.py"
    output_dir = os.path.join(reproc_home, dqr, "datastream", site)
    cmd = "{}/{}".format(output_dir, "*")
    if DEBUG:
        print("\t" + cmd)
    output_dirs = glob(cmd)
    if DEBUG:
        print("Output directories:")
        for output_dir in output_dirs:
            print("\t{}".format(output_dir))
    print("Finished ncreview setup.\n")
    return output_dirs, ncr_cmd


def run_ncreview(dqr: str, site: str, comp_dir: str) -> None:
    """Run the ncreview command in each of the output directories.
    The ncreview command will not be run in 00 level data directories because those files
    are not cdf files and ncreview would fail. Use a regex to search for the link to the
    ncreview and print it to the console.

    :param dqr: string representing the dqr number of this job.
    :param site: ARM site that the datastream came from.
    :return: None
    """
    print("Running ncreveiw... ")
    output_dirs, ncr_cmd = ncreview_setup(dqr, site, comp_dir)
    for output_dir in output_dirs:
        if output_dir[-2:] != "00":
            output_datastream_folder = output_dir.split("/")[-1]
            print(output_datastream_folder)
            os.chdir(output_dir)
            proc = subprocess.Popen(ncr_cmd, shell=True, stdout=subprocess.PIPE)
            out, err = proc.communicate()
            print("\t{} Errors: {}".format(output_datastream_folder, err))

            result = LINK_REGEX.search(str(out))
            if result:
                # print contents of log file to console
                print("\tNcreview link: {}".format(result.group()))
            else:
                print("\tNo ncreview link available.")
    print("Finished running ncreview.\n")


def data_dictionary(dqr: str) -> dict:
    """Create dictionary from changes to cdf files
    todo Finish dictionary workflow
    --- this workflow will be more complicated than just one method. ---
    - setup method to get original and modified cdf files
    - scan files for changes, this work already done in "auto_var_data_dict.ipynb
    - create/open dict and merge current changes
    - add new element to dict
    - copy old file to backup
    - write output file

    :param dqr: string representing the dqr number of this job.
    :return: dict or json of data raw to cdf data information
    """
    # get reprocessing environment variables
    reproc_home, _, _ = reproc_env(dqr)
    # automatically create/append to the data dictionary
    dict_path = os.path.join(reproc_home, "working_data_dictionaries")
    print(dict_path)
    return dict()


def cleanup_datastream(dqr: str, site: str) -> None:
    """Cleanup the datastream directory
    Remove all files and folders in the reproc home/dqr/datastream/site directory.
    This is done to keep the folder clean for the next round of modifications in
    case a new time range is staged and tested, otherwise there will be leftover
    files that ncreview will try to compare. This stage can be skipped with the
    --no-clean command line flag.

    :param dqr: string representing the dqr number of this job.
    :param site: ARM site that the datastream came from.
    :return: None
    """
    # get reprocessing environment variables
    reproc_home, _, _ = reproc_env(dqr)
    print("Cleaning datastream directories... ")
    rm_path = os.path.join(reproc_home, dqr, "datastream", site)
    os.system("rm -rvf {}".format(rm_path))
    print("Finished cleanup.\n")


def restage_files(input_dir: str) -> None:
    """Move files from backup directory into collection/ingest directory
    For the ingest to work it expects the input files to be in a specific
    directory structure that can't be changed. We mimic this structure in
    the reprocessing environment.

    :param input_dir: the directory of the original input files
    :return: None
    """
    autotest_dir = os.path.join(input_dir, ".autotest")
    print("Re-staging files from autotest directory... ", end="")
    orig_files = os.listdir(autotest_dir)
    for file_name in orig_files:
        src = os.path.join(autotest_dir, file_name)
        dest = os.path.join(input_dir, file_name)
        shutil.move(src, dest)
    print("Done re-staging files.\n")


def main():
    """Main workflow
    This should read like a high level overview of what is being done
    with some minor management behaviors.

    :return: None
    """
    args = parse_args()
    # try and get arguments from path
    dqr = DQR_REGEX.search(args.input).group()
    datastream = DATASTREAM_REGEX.search(args.input).group()

    if args.interactive:
        # ask if arguments are correct?
        question = "DQR # = {}\nRaw datastream = {}\nIs this correct? ".format(dqr, datastream)
        if input(question) in ['y', 'yes', 'yea', 'ok']:
            pass
        # else ask for arguments
        else:
            dqr = input("Enter the DQR #:\nEXAMPLE D180042.4: ")
            datastream = input("Enter the datastream:\nEXAMPLE sgp30ebbrC1.00: ")

    site, instrument, facility = parse_datastream(datastream)
    print(site, instrument, facility)
    print("\tPreceding with test.")

    # set environment variables for reprocessing
    setup_environment(dqr)

    # get files to modify
    files = get_files(os.path.join(args.input, '*'))

    # copy input files to backup directory
    backup_input_files(args.input, files)

    # This is tentatively where the loop for each column will occur #

    # cleanup post processing of old ncreview files
    cleanup_postproc(dqr)

    # run modification procedure
    modify_files(args, files)

    # run ingest
    ingest_files(site, datastream)

    # run ncreveiw
    run_ncreview(dqr, site, args.comp_dir)

    # make that sweet sweet data dictionary
    # data_dictionary(dqr) # TODO Finish this function

    # cleanup datastream directory
    if not args.no_clean:
        cleanup_datastream(dqr, site)

    # re-stage raw files from backup directory
    restage_files(args.input)

    # repeat


if __name__ == "__main__":
    main()

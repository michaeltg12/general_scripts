#!/apps/base/python3/bin/python3

import argparse
import os, shutil
import subprocess
from glob import glob
import numpy as np
import netCDF4 as nc4
import csv
import re

DEBUG = True

dqr_regex = re.compile("D\d{6}(\.)*(\d)*")
datastream_regex = re.compile("(acx|awr|dmf|fkb|gec|hfe|mag|mar|mlo|nic|nsa|osc|pgh|pye|sbs|shb|tmp|wbu|zrh|asi|cjc|ena|gan|grw|isp|mao|mcq|nac|nim|oli|osi|pvc|rld|sgp|smt|twp|yeu)\w+\.(\w){2}")
date_regex = re.compile("[1,2]\d{7}")
link_regex = re.compile(r"https:([^\\]*)")

help_description = '''
This program will automate testing the variable mapping from raw to cdf/nc files.
It must be run from the directory where the input raw files are.
'''

example = '''
EXAMPLE: TODO
'''

def parse_args():
    #Information needed
    #DQR # --> get from path
    #cleanup old ncreview files
    #assume bash
    #location of raw input data
    #    assume 1 input data
    #location of modification scripts
    #ingest and command
    #clean up 
    #    datastream direcotory
    parser = argparse.ArgumentParser(description=help_description, epilog=example,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input', dest='input', type=str, default=os.getcwd(),
                        help='input directory, ex. /data/archive/sgp/sgpmetE13.00')
    parser.add_argument('-m', '--modify', dest='modify', type=int, 
                        help='Column to modify and test.')
    parser.add_argument('--create-dict', dest='create_dict', action="store_true")
    parser.add_argument('--delimiter', dest='delimiter', default=',', help='File delimiter.')
    parser.add_argument('--header', dest='header', type=int, default=0, help='Number of header lines.')
    parser.add_argument('--skip-col', dest='skip_column', nargs='+', type=int, default=[], help='Columns to skip. Must be last argument.')
    parser.add_argument('-I', '--Interactive', action='store_true', dest='interactive', default=False,
                        help='Interactive / partial execution')
    
    args = parser.parse_args()
    if args.modify in args.skip_column:
        print('Skipping modification column, no effect.')
        exit(0)
    if args.create_dict:
        if not args.header:
            args.header = input("Number of header lines = ")
        if not args.skip_column:
            args.skip_column = eval(input("Columns to skip = "))

    return args

def parse_datastream(datastream):
    site = datastream[:3]
    instrument = datastream[3:-2]
    facility = datastream[-2:]
    return site, instrument, facility

<<<<<<< HEAD
def reproc_env(dqr):
    reproc_home = os.getenv("REPROC_HOME")  # expected to be the current reproc environment
    post_processing = os.getenv("POST_PROC")  # is a post processing folder under reproc home
    data_home = f"{reproc_home}/{dqr}"  # used to set environment variables for current dqr job
    return reproc_home, post_processing, data_home

def setup_environment(dqr):
    # get reprocessing environment variables
    reproc_home, post_processing, data_home = reproc_env(dqr)
=======
def main():
    args = parse_args()
    # try and get arguments from path
    cwd = os.getcwd()
    dqr = dqr_regex.search(cwd).group()
    datastream = datastream_regex.search(cwd).group()

    if args.interactive:
        # ask if arguments are correct?
        question = "DQR # = {}\nRaw datastream = {}\nIs this correct? ".format(dqr, datastream)
        if input(question) in ['y', 'yes', 'yea', 'ok']:
            pass
        # else ask for arguments
        else:
            dqr = input("Enter the DQR #:\nExample D180042.4: ")
            datastream = input("Enter the datastream:\nExample sgp30ebbrC1.00: ")

    site, instrument, facility = parse_datastream(datastream)
    print(site, instrument, facility)
    print("\tProceding with test.")

    # source environment variables
    reproc_home = os.getenv("REPROC_HOME") # expected to be the current reproc environment
    post_processing = os.getenv("POST_PROC") # is a post processing folder under reproc home
    data_home = f"{reproc_home}/{dqr}" # used to set environment variables for current dqr job 
>>>>>>> parent of 19ae176... modified the whole program to be more modular definition based. This will hopefully help me make unit tests and make it easier to change to automaticaly creat the data dictionary

    # try sourcing by apm created environment file in case it has more than the default
    env_file = os.path.join(reproc_home, dqr, 'env.bash')
    if os.path.isfile(env_file) and False: #TODO HACK!! Error with this env source system
        with open(env_file, 'r') as open_env_file:
            print("\nAttempting to source from local env.bash file...")
            lines = open_env_file.readlines()
            for l in lines:
                key, value = l.split("=")
                value = value[1:-1].replace("DATA_HOME", data_home)
                print("{}={}".format(key, value))
                os.environ[key] = value
    else:
        # set environment variables based on this default
        env_vars = {"DATA_HOME" : data_home,
        "DATASTREAM_DATA" : f"{data_home}/datastream",
        "ARCHIVE_DATA" : "/data/archive",
        "OUT_DATA" : f"{data_home}/out",
        "TMP_DATA" : f"{data_home}/tmp",
        "HEALTH_DATA" : f"{data_home}/health",
        "QUICKLOOK_DATA" : f"{data_home}/quicklooks",
        "COLLECTION_DATA" : f"{data_home}/collection",
        "CONF_DATA" : f"{data_home}/conf",
        "LOGS_DATA" : f"{data_home}/logs",
        "WWW_DATA" : f"{data_home}/www",
        "DB_DATA" : f"{data_home}/db"}

        #print("\nEnvironment file does not exist at:\n\t{}".format(env_file))
        print("\nSourcing from default dict:")
        for key, value in env_vars.items():
            print("\t{}={}".format(key, value))
            os.environ[key] = value

    # get files for modification
    file_search = os.path.join(args.input, '*')
    files = glob(file_search)
    # get date from files to look for ingest command and for cdf comparison later
    for f in files:
        result_date = date_regex.search(f.split('/')[-1])
        if result_date:
            result_date = result_date.group()
            break
    else:
        print("No result date found. Exiting")
        exit(1)

    # get ingest command
    ingest_search = os.path.join("/data/archive/", site, datastream[:-3]+"*")
    print("\nSearching for output directories:\n\t{}".format(ingest_search))
    cmd = 'ls -d {}'.format(ingest_search)
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    out, err = proc.communicate()
    out_2string_striped = str(out)[2:-3]
    out_split = out_2string_striped.split('\\n')
    print("Found the following directories:\n\t{}".format(out_split))
    for element in out_split:
        if element[-2:] != '00':
            search_dir = os.path.join("/data/archive", site, element, "*"+result_date+"*")
            print('Searching for netcdf file:\n\t{}'.format(search_dir))
            ingested_file = glob(search_dir)[0]
            cmd = "ncdump -h {} | grep command".format(ingested_file)
            print(cmd)
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            out, err = proc.communicate()
            ingest_command = str(out).split('"')[1]
            if ingest_command:
                print("Ingest command: {}\n".format(ingest_command))
                break

    # copy input files to backup directory
    print("Coping files into .autotest dir...")
    autotest_dir = os.path.join(cwd, ".autotest")
    if not os.path.exists(autotest_dir):
        os.makedirs(autotest_dir)
        print('Created directory: {}'.format(autotest_dir))
    mod_files = []
    for f in files:
        src = os.path.join(cwd, os.path.basename(f))
        dest = os.path.join(autotest_dir, os.path.basename(f))
        try:
            if not os.path.exists(dest):
                shutil.copy(src, dest)
                print("\tMoved: {}".format(src))
            else: 
                print("\tExits: {}".format(dest))
            mod_files.append(dest)
        except shutil.SameFileError:
            print("\tExits: {}".format(dest))
            pass
    print("Finished copying files.\n")

<<<<<<< HEAD
def cleanup_postproc(dqr):
    _, post_processing, _ = reproc_env(dqr)
=======
    ### TODO This is where the loop for each column will occur ###
    
    # TODO This won't be necessary in the end when it auto compares the cdf files
    # cleanup post processing of old ncreview files
>>>>>>> parent of 19ae176... modified the whole program to be more modular definition based. This will hopefully help me make unit tests and make it easier to change to automaticaly creat the data dictionary
    print("Cleaning up old ncreview files")
    rm_path = os.path.join(post_processing, dqr, 'ncr*')
    os.system("rm -rvf {}".format(rm_path))
    print("Finished cleanup.\n")
    
    # run modification procedure
    print("Setting up to modify files... ", end="")
    # set header rows
    if args.header:
        rows_to_skip = [x for x in range(args.header)]
    else:
        rows_to_skip = []
    # set columns to modify or default for automatically doing the whole file
    # columns_to_skip = args.skip_column

    print("Finished setup.\n")

    print("Modifying files... ", end="")
    for i, input_file in enumerate(files):
        output_list = []
        with open(input_file) as open_input_file:
            csv_reader = csv.reader(open_input_file, delimiter=args.delimiter)
            for j, line in enumerate(csv_reader):
                if j not in rows_to_skip:
                    try:
                        line[args.modify] = eval(line[args.modify]) + 1000
                    except IndexError:
                        pass
                    output_list.append(line)
                else: output_list.append(line)
        output_file = input_file.split("/")[-1]
        with open(output_file, 'w') as open_output_file:
            csv_writer = csv.writer(open_output_file)
            csv_writer.writerows(output_list)
    print("Finished modifying files.\n")

    # run ingest 
    print("Running ingest command: <{}> ... ".format(ingest_command), end="")
    proc = subprocess.Popen(ingest_command, shell=True, stdout=subprocess.PIPE)
    out, err = proc.communicate()
    print("Finished running ingest.\n\tErrors: {}\n".format(err))

<<<<<<< HEAD

def ncreview_setup(dqr, site):
    # get reprocessing environment variables
    reproc_home, _, _ = reproc_env(dqr)
=======
    # setup for ncreview *** TODO setup for cdf comparison ***
>>>>>>> parent of 19ae176... modified the whole program to be more modular definition based. This will hopefully help me make unit tests and make it easier to change to automaticaly creat the data dictionary
    print("Setting up for ncreveiw... ")
    ncr_cmd = "python3.6 /data/project/0021718_1509993009/ADC_Reproc_Toolbox/bin/ncr_cmd.py"
    output_dir = os.path.join(reproc_home, dqr, "datastream", site)
    cmd = "{}/{}".format(output_dir, "*")
    if DEBUG: print("\t" + cmd)
    output_dirs = glob(cmd)
    if DEBUG: 
        for d in output_dirs:
            print("\t{}".format(d))
    print("Finished ncreview setup.\n")

<<<<<<< HEAD
            # print contents of log file to console *** TODO add results to json file ***
            print("\tNcreview link: {}".format(link_regex.search(str(out)).group()))
    print("Finished running ncreview.\n")

def cleanup_datastream(dqr, site):
    # get reprocessing environment variables
    reproc_home, _, _ = reproc_env(dqr)
=======
    if not args.create_dict:
        # run ncreveiw *** TODO evaluate cdf comparison ***
        print("Running ncreveiw... ")
        for output_dir in output_dirs:
            if output_dir[-2:] != "00":
                ds = output_dir.split("/")[-1]
                os.chdir(output_dir)
                proc = subprocess.Popen(ncr_cmd, shell=True, stdout=subprocess.PIPE)
                out, err = proc.communicate()
                print("\t{} Errors: {}".format(ds, err))

                # print contents of log file to console *** TODO add results to json file ***
                print("\tNcreview link: {}".format(link_regex.search(str(out)).group()))
        print("Finished running ncreview.\n")
    else:
        # automatically create/append to the data dictionary
        default_dict = {"header": {}, "data": {}, "coeff": {}}
        dict_filename = instrument
        dict_path = os.path.join(reproc_home, "working_data_dictionaries")
        if not os.path.exists(dict_path):
            os.makedirs(dict_path)
        # TODO finishe this stuff.
        # full auto dict generation will require a new workflow not exatly supported by the indifidual column modification
        # think of making another module that creates the dict. 
    
    # cleanup datastream direcotry
>>>>>>> parent of 19ae176... modified the whole program to be more modular definition based. This will hopefully help me make unit tests and make it easier to change to automaticaly creat the data dictionary
    print("Cleaning datastream direcories... ")
    rm_path = os.path.join(reproc_home, dqr, "datastream", site)
    os.system("rm -rvf {}".format(rm_path))
    print("Finished cleanup.\n")

<<<<<<< HEAD
def restage_files(input_dir):
    autotest_dir = os.path.join(input_dir, ".autotest")
=======
    # restage raw files from backup directory
>>>>>>> parent of 19ae176... modified the whole program to be more modular definition based. This will hopefully help me make unit tests and make it easier to change to automaticaly creat the data dictionary
    print("Restaging files from autotest directory... ", end="")
    orig_files = os.listdir(autotest_dir)
    for f in orig_files:
        src = os.path.join(autotest_dir, f)
<<<<<<< HEAD
        dest = os.path.join(input_dir, f)
=======
        dest = os.path.join(cwd, f)
>>>>>>> parent of 19ae176... modified the whole program to be more modular definition based. This will hopefully help me make unit tests and make it easier to change to automaticaly creat the data dictionary
        shutil.move(src, dest)
    print("Done re-staging files.\n")

<<<<<<< HEAD
def data_dictionary(dqr):
    # get reprocessing environment variables
    reproc_home, post_processing, data_home = reproc_env(dqr)
    # automatically create/append to the data dictionary
    dict_path = os.path.join(reproc_home, "working_data_dictionaries")
    print(dict_path)
    # TODO finish this stuff.
    """ 
    full auto dict generation will require a new workflow not exactly supported by the individual column modification  
    think of making another module that creates the dict. 
    """

def main():
    args = parse_args()
    # try and get arguments from path
    dqr = dqr_regex.search(args.input).group()
    datastream = datastream_regex.search(args.input).group()

    if args.interactive:
        # ask if arguments are correct?
        question = "DQR # = {}\nRaw datastream = {}\nIs this correct? ".format(dqr, datastream)
        if input(question) in ['y', 'yes', 'yea', 'ok']:
            pass
        # else ask for arguments
        else:
            dqr = input("Enter the DQR #:\nExample D180042.4: ")
            datastream = input("Enter the datastream:\nExample sgp30ebbrC1.00: ")

    site, instrument, facility = parse_datastream(datastream)
    print(site, instrument, facility)
    print("\tProceding with test.")

    # set environment variables for reprocessing
    setup_environment(dqr)

    # get files to modify
    files = get_files(os.path.join(args.input, '*'))

    # copy input files to backup directory
    backup_input_files(args.input, files)

    ### TODO This is tentatively where the loop for each column will occur ###

    # cleanup post processing of old ncreview files
    cleanup_postproc(dqr)
    
    # run modification procedure
    modify_files(args, files)

    # run ingest 
    ingest_files(files, site, datastream)

    # setup for ncreview *** TODO setup for cdf comparison ***
    output_dirs, ncr_cmd = ncreview_setup(dqr, site)

    # run ncreveiw *** TODO evaluate cdf comparison ***
    run_ncreview(output_dirs, ncr_cmd)

    # make that sweet sweet data dictionary
    # data_dictionary(dqr) # TODO This stuff
    
    # cleanup datastream direcotry
    cleanup_datastream(dqr, site)

    # restage raw files from backup directory
    restage_files(args.input)

=======
>>>>>>> parent of 19ae176... modified the whole program to be more modular definition based. This will hopefully help me make unit tests and make it easier to change to automaticaly creat the data dictionary
    # repeat

if __name__ == "__main__":
    main()


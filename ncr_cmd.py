""" Ncreview Command Automator
author: Michael Giansiracusa
email: giansiracumt@ornl.gov

This module will facilitate running ncreview, cleaning output directory and archiving
the necessary files to the post processing directory as specified by the shell environment
variable `POST_PROC`.

Example: None yet

Attributes:
    DEBUG (bool): True to print debug messages
    DQR_REGEX (raw str): regex to search for dqr # in cwd path string
    DATASTREAM_REGEX (raw str): regex to search for datastream name in cwd path string
    HELP_DESCRIPTION (str): help description for argparse
    EXAMPLE (str): example for argparse
"""

import os
import re
import shutil
import argparse
from glob import glob

DQR_REGEX = re.compile(r"D\d{6}(\.)*(\d)*")
DATASTREAM_REGEX = re.compile(r"(acx|awr|dmf|fkb|gec|hfe|mag|mar|mlo|nic|nsa|osc|pgh|pye|sbs|shb|"
                              r"tmp|wbu|zrh|asi|cjc|ena|gan|grw|isp|mao|mcq|nac|nim|oli|osi|pvc|"
                              r"rld|sgp|smt|twp|yeu)\w+\.(\w){2}")

HELP_DESCRIPTION = '''
This program will facilitate using ncreview. It will build and run the ncreview command, clean
the post processing directory of previous ncreviews and archive the current run given standard
naming conventions. It assumes that the current directory is where the cdf files to compare are
located and that the DQR number and datastream name are both in the current path. If the path
does not contain this information it can be specified on the command line. The DQR number will
be used to create a folder in the post processing directory and doens't have to be a valid DQR
number, or a number at all. Because of this option, a generic JOB_NAME can be specified. The 
datastream must be a valid datastream and is used to access files in the archive.
'''

EXAMPLE = '''
EXAMPLE: TODO
'''

PARSER = argparse.ArgumentParser(description=HELP_DESCRIPTION, epilog=EXAMPLE,
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

PARSER.add_argument('-i', '--input', dest='input', type=str, default=os.getcwd(),
                    help='input directory, ex. /data/project/reproc/D123456.1/sgpmetE13.b1')
PARSER.add_argument('-b', '--begin', dest='begin', type=int, default=0,
                    help='Ignore files before YYYYMMDD')
PARSER.add_argument('-e', '--end', dest='end', type=int, default=0,
                    help='Ignore files after YYYYMMDD')
PARSER.add_argument('--no-trim', dest='no_trim', action='store_true',
                    help='Show full range of both DATASTREAMs')
PARSER.add_argument('-t', '--sample-interval', dest='sample_interval', type=str, default='',
                    help='Time interval to average data over in HH-MM-SS. If not '
                         'provided, defaults to 1 day if more than 10 days are'
                         ' being processed, otherwise defaults to hourly samples.')
PARSER.add_argument('--job-name', dest='JOB_NAME', type=str, default='',
                    help='Shold be a DQR number for archiving ncreview into post processing'
                         'directory. Could be any job name but that will break conventions.')
PARSER.add_argument('-ds', '--datastream', dest='DATASTREAM', default='',
                    help='The name of the directory that the cdf files are in, ex. sgpmetE13.b1')
PARSER.add_argument('--cleanup', dest="clean", action="store_true",
                    help="CAREFUL, this will clean all ncreview folders and LOGs.")

ARGS = PARSER.parse_args()

if ARGS.JOB_NAME:
    JOB_NAME = ARGS.JOB_NAME
else:
    JOB_NAME = DQR_REGEX.search(ARGS.input).group()
if ARGS.DATASTREAM:
    DATASTREAM = ARGS.DATASTREAM
else:
    DATASTREAM = DATASTREAM_REGEX.search(ARGS.input).group()
SITE = DATASTREAM[:3]

POST_PROC = os.environ['POST_PROC']

OUT_DIR = "{}/{}".format(POST_PROC, JOB_NAME)
NCR_DIR = "{}/ncr_{}".format(OUT_DIR, DATASTREAM)


if not os.path.isdir(OUT_DIR):
    print("Making output directory @ {}".format(OUT_DIR))
    os.makedirs(OUT_DIR)

if ARGS.clean:
    print("Cleaning post processing directory - {}".format(OUT_DIR))
    print("\tRemoving files/folders matching - ncr_{}*".format(DATASTREAM))
    RESULTS = glob("{}/ncr_{}*".format(OUT_DIR, DATASTREAM))
    for result in RESULTS:
        if os.path.isfile(result):
            os.remove(result)
        elif os.path.isdir(result):
            shutil.rmtree(result)
        else:
            print("Skipping - {}".format(result))

LOG = "{}/{}/ncr_{}.log".format(POST_PROC, JOB_NAME, DATASTREAM)
while os.path.isfile(LOG):
    try:
        NUM = int(LOG) + 1
        LOG = "{}{}".format(LOG[:-1], NUM)
    except ValueError:
        LOG = "{}1".format(LOG)

if ARGS.begin:
    ARGS.begin = "-b {}".format(ARGS.begin)
else:
    ARGS.begin = ""
if ARGS.end:
    ARGS.end = "-e {}".format(ARGS.end)
else:
    ARGS.end = ""
if ARGS.sample_interval:
    if re.search(r"\d{2}-\d{2}-\d{2}", ARGS.sample_interval):
        ARGS.sample_interval = "-t {}".format(ARGS.sample_interval)
    else:
        print("Sample interval needs to be of the form 12-23-34. Omiting this parameter")


CMD = "ncreview /data/archive/{}/{}/ {} {} {} {}-n ncr_{} -w {} >> {}"\
    .format(SITE, DATASTREAM, ARGS.input, ARGS.begin, ARGS.end,
            ARGS.sample_interval, DATASTREAM, OUT_DIR, LOG)
print("CMD = {}".format(CMD))

os.system(CMD)
os.system(f"cat {LOG}")

#!/apps/base/python3/bin/python3

from netCDF4 import Dataset
from glob import glob
import argparse

help_description = """
This is a first go at changing the hatch open status of the aeri datastreams
"""
example = """
python aeri_hatch_status.py -d <dqr> -r <rid> -D
"""
def get_args():
    parser = argparse.ArgumentParser(description=help_description, epilog=example,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-d', '--dqr', type=str, dest='dqr', help='dqr to append to new reproc global attribute')
    parser.add_argument('-r', '--rid', type=str, dest='rid', help='rid to append to new reproc global attribute')
    parser.add_argument('-D', '--debug', action='store_true', dest='debug', help='print debug statements.')

    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = get_args()
    for f in glob("*.cdf"):
        d = Dataset(f,'r+')
        new_status = []
        try:
            for x in d.variables['hatchOpen'][:]:
                if x == 0:
                    new_status.append(1)
                elif x == 1:
                    new_status.append(0)
                else:
                    new_status.append(x)
            d.variables['hatchOpen'][:] = new_status
        except AttributeError:
            print("hatchOpen Attribute not found on file {}".format(f))
        try:
            msg = "{} - RID {}".format(args.dqr, args.rid)
            if d.reproc == msg:
                pass
            else:
                d.reproc = "{} : {} - RID {}".format(d.reproc, args.dqr, args.rid)
        except AttributeError:
            d.reproc = msg
        d.close()

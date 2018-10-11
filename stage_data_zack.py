import logging
import logging.config
from argparse import ArgumentParser

import requests

from stage_arm_data.cli import valid_timestamp
from stage_arm_data.core import transfer_files
from stage_arm_data.endpoints import Stratus


def get_matching_rulesets(run_location, rulesets):
    '''Filter out rules for other sites. Only return rules for this site
    and rules that match ALL sites.'''
    wild_card = {'site': 'RUN_LOCATION', 'fac': None}

    return [
        rule for rule in rulesets
        if rule['run_location'] == wild_card
        or rule['run_location'] == run_location
    ]


parser = ArgumentParser()
parser.add_argument('--site', action='store', required=True)
parser.add_argument('--facility', action='store', required=True)
parser.add_argument('--start', action='store', required=True, type=valid_timestamp)
parser.add_argument('--end', action='store', required=True, type=valid_timestamp)
args = parser.parse_args()

proc_config = requests.get('https://pcm.arm.gov/pcmserver/processes/aerioe', verify=False).json()
input_datasets = proc_config['process']['variable_retrieval']['input_datasets'].values()
allowed_run_locations = proc_config['process']['run_locations']

run_location = {'site': args.site, 'fac': args.facility}
if run_location not in allowed_run_locations:
    raise ValueError(f'{run_location} is not allowed for this process. Allowed locations: {allowed_run_locations}')

required_datastreams = []
for dataset in input_datasets:
    for rule in get_matching_rulesets(run_location, dataset['rules']):
        dsclass, data_level = rule['datastream_name'].split('.')

        ds_site = rule['data_location']['site'].lower()
        if ds_site == 'run_location':
            ds_site = args.site.lower()

        ds_facility = rule['data_location']['fac'] or args.facility
        full_datastream_name = f'{ds_site}{dsclass}{ds_facility}.{data_level}'
        required_datastreams.append(full_datastream_name)

for ds in required_datastreams:
    transfer_files({'datastream': ds, 'start_time': args.start, 'end_time': args.end}, Stratus)

import hashlib
import time
from datetime import datetime

from helper.mongo_helper import *

from gnfg_engine.aca_engine import initiate_aca as ACA

modules = {
    'ACA': ACA
}


def get_meta_data(job_id, module_names=None):
    if module_names is None:
        module_names = []

    return {
        'name': job_id,
        'start_time': None,
        'end_time': None,
        'status': 'PreProcessing',
        'modules': {
            module_name: {
                'start_time': None,
                'end_time': None,
                'status': 'Pending',
            }
            for module_name in module_names
        }
    }


def start(graph_config, output_file=None, VERBOSE=False, job_id=None):
    if not job_id:
        hashed = hashlib.sha1()
        hashed.update(str(time.time()).encode('utf-8'))
        job_id = hashed.hexdigest()

    job_meta_data = get_meta_data(job_id, list(modules.keys()))

    job_meta_data['status'] = 'Running'
    job_meta_data['start_time'] = datetime.now()
    write_to_mongo(job_meta_data)

    for module_name in modules:
        module = modules[module_name]

        job_meta_data['modules'][module_name]['status'] = 'Running'
        job_meta_data['modules'][module_name]['start_time'] = datetime.now()
        write_to_mongo(job_meta_data)

        module.start(graph_config, output_file, VERBOSE)

        job_meta_data['modules'][module_name]['status'] = 'Completed'
        job_meta_data['modules'][module_name]['end_time'] = datetime.now()
        write_to_mongo(job_meta_data)

    job_meta_data['status'] = 'Completed'
    job_meta_data['end_time'] = datetime.now()
    write_to_mongo(job_meta_data)

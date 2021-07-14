import hashlib
import time
from datetime import datetime

from helper.mongo_helper import *

from gnfg_engine.aca_engine.initiate_aca import ACAEngine

modules_list = {
    'ACA': ACAEngine
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


def start(graph_config, job_id=None, **kwargs):
    if not job_id:
        hashed = hashlib.sha1()
        hashed.update(str(time.time()).encode('utf-8'))
        job_id = hashed.hexdigest()

    job_meta_data = get_meta_data(job_id, list(modules_list.keys()))
    data_to_return = {}

    job_meta_data['status'] = 'Running'
    job_meta_data['start_time'] = datetime.now()
    write_to_mongo(job_meta_data)

    for module_name in modules_list:
        module_class = modules_list[module_name]
        module_object = module_class(graph_config, **kwargs)

        job_meta_data['modules'][module_name]['status'] = 'Running'
        job_meta_data['modules'][module_name]['start_time'] = datetime.now()
        write_to_mongo(job_meta_data)

        temp_data = module_object.start()

        job_meta_data['modules'][module_name]['status'] = 'Completed'
        job_meta_data['modules'][module_name]['end_time'] = datetime.now()
        write_to_mongo(job_meta_data)

        data_to_return[module_name] = {}
        data_to_return[module_name]['meta_data'] = job_meta_data
        data_to_return[module_name]['data'] = temp_data

    job_meta_data['status'] = 'Completed'
    job_meta_data['end_time'] = datetime.now()
    write_to_mongo(job_meta_data)

    return data_to_return

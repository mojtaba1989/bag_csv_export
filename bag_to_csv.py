import sys
if sys.version_info < (3, 10):
    raise Exception("Python 3.10 or newer required")

from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore
from pathlib import Path
from itertools import cycle

import numpy as np

import argparse
import multiprocessing as mp
import os
import json

file_path = os.path.abspath(__file__)
dir_path = os.path.dirname(file_path)
default_config_path = os.path.join(dir_path, 'config.json')
typestore = get_typestore(Stores.ROS1_NOETIC)

def get_nested_attr(obj, attr_path, ignore_first=True):
    if obj is None:
        return globals()[attr_path]
    attrs = attr_path.split('.')
    if ignore_first:
        attrs = attrs[1:]
    for attr in attrs:
        obj = getattr(obj, attr)
        if obj is None:
            return None
    return obj

def check_config(config_file):
    try:
        with open(config_file) as json_data_file:
            config = json.load(json_data_file)
        return config
    except:
        return None
    
def vprint(msg, verbose=False):
    if verbose:
        print(msg)                                                                           

def gen_csv_task(args):
    worker_id, task = args
    bag_name, bag_dir,  target_dir, config, config_key, overwrite, verbose = task

    FILE_NAME = os.path.join(target_dir, '.'.join([bag_name, config_key]))
    BAG_NAME = os.path.join(bag_dir, bag_name)

    try:
        if os.path.getsize(FILE_NAME) > 0 and (overwrite == 'Never' or overwrite == 'Ask'):
            return
        else:
            pass
    except:
        pass
    vprint("[Worker {}] Processing {} | {}".format(worker_id, config_key, bag_name), verbose)


    with open(FILE_NAME, 'w') as f, AnyReader([Path(BAG_NAME)], default_typestore=typestore) as reader:
        connectios = [x for x in reader.connections if x.topic == config['topic']]
        if len(connectios) == 0:
            vprint("[Worker {}] Failed {} | {}".format(worker_id, config_key, bag_name), verbose)
            return
        f.write('time,')
        f.write(','.join(config['cols'])+'\n')
        if "arrays" in config.keys():
            for c, t, raw_msg in reader.messages(connections=connectios):
                msg = reader.deserialize(raw_msg, c.msgtype)
                for attr in get_nested_attr(msg, config["arrays"]["field"]) or []:
                    row = [get_nested_attr(attr, fld) for fld in config["fields"]]
                    row.insert(0, t)
                    f.write(','.join(str(i) for i in row))
                    f.write('\n')
        else:
            for c, t, raw_msg in reader.messages(connections=connectios):
                msg = reader.deserialize(raw_msg, c.msgtype)
                row = [get_nested_attr(msg, fld) for fld in config["fields"]]
                f.write('{},'.format(t))
                f.write(','.join(str(i) for i in row))
                f.write('\n')
    try:
        if os.path.getsize(FILE_NAME) > 0:
            msg = "[Worker {}] Completed {} | {}".format(worker_id, config_key, bag_name)
            vprint(msg, verbose)            
            if not verbose:
                print(msg, end='\r', flush=True) 
        else:
            os.remove(FILE_NAME)
            vprint("[Worker {}] Failed {} | {}".format(worker_id, config_key, bag_name), verbose)
    except:
        vprint("[Worker {}] Failed {} | {}".format(worker_id, config_key, bag_name), verbose)
    
    return

def gen_csv_multiprocessing(BAG_DIR, config_file=default_config_path, num_workers=1, overwrite='Always', verbose=False):
    if not os.path.exists(os.path.join(BAG_DIR, 'csv')):
            os.makedirs(os.path.join(BAG_DIR, 'csv'))
    TARGET_DIR = os.path.join(BAG_DIR, 'csv')
    vprint('Using bag folder: {}'.format(BAG_DIR), verbose)
    vprint('Using target directory: {}'.format(TARGET_DIR), verbose)

    config = check_config(config_file)
    if config is None:
        vprint('Configuration file is invalid!', verbose)
        exit(4)
    vprint('Using config: {}'.format(config_file), verbose)

    vprint('Selected overwrite option: {}'.format(overwrite), verbose)
    if num_workers <= 1:
        num_workers = 1
        vprint('Parallelization is disabled.', verbose)
    else:
        num_workers = min(num_workers, mp.cpu_count())
        vprint('Number of cores: {} (out of {})'.format(num_workers, mp.cpu_count()), verbose)

    bag_lst = [bag for bag in os.listdir(BAG_DIR) if bag.endswith('.bag')]
    tasks = []
    for bag_name in bag_lst:
        for config_key in config.keys():
            tasks.append((bag_name, BAG_DIR, TARGET_DIR, config[config_key], config_key, overwrite, verbose))

    vprint("Number of tasks: {}".format(len(tasks)), verbose)

    task_queue = [(worker_id, task) for worker_id, task in zip(cycle(range(num_workers)), tasks)]
    with mp.Pool(num_workers) as pool:
        pool.map(gen_csv_task, task_queue)
        

    print("All tasks completed.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate CSV files from bag files.')
    parser.add_argument('dir', type=str, help='Bag files directory')
    parser.add_argument('-c', '--config_file', type=str, default=default_config_path, help='Config file')
    parser.add_argument('-w', '--num_workers', type=int, default=mp.cpu_count(), help='Number of workers')
    parser.add_argument('-o', '--overwrite', type=str, default='Never', choices=['Always', 'Never', 'Ask'], help='Overwrite csv file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose mode')

    args = parser.parse_args()
    gen_csv_multiprocessing(args.dir, args.config_file, args.num_workers, args.overwrite, args.verbose)

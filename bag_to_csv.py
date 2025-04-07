from __future__ import print_function
import os
import json
import rosbag
import numpy as np
import rospy
import sys
import argparse

file_path = os.path.abspath(__file__)
dir_path = os.path.dirname(file_path)
default_config_path = os.path.join(dir_path, 'config.json')

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

BAG_DIR = r"/run/user/1000/gvfs/smb-share:server=acm_mtu_strg.local,share=homes/D-ADS/3-11-25"

def check_config(config_file):
    try:
        with open(config_file) as json_data_file:
            config = json.load(json_data_file)
        return config
    except:
        return None

def gen_csv(BAG_DIR, config_file=default_config_path, overwrite='Always'):
    if not os.path.isfile(config_file):
        print('Invalid config file!')
        exit(1)
    else:
        print('Using config file: {}'.format(config_file))

    if not os.path.isdir(BAG_DIR):
        if os.path.isfile(BAG_DIR) and BAG_DIR.endswith('.bag'):
            bag_lst = [BAG_DIR]
            BAG_DIR = '.'
            parent_dir = os.path.dirname(BAG_DIR)
            if not os.path.exists(os.path.join(parent_dir, 'csv')):
                os.makedirs(os.path.join(parent_dir, 'csv'))
            TARGET_DIR = os.path.join(parent_dir, 'csv')
            print('Using bag file: {}'.format(BAG_DIR))
            print('Using target directory: {}'.format(TARGET_DIR))
        else:
            print('Invalid bag folder or file!')
            exit(2)
    else:
        bag_lst = [bag for bag in os.listdir(BAG_DIR) if bag.endswith('.bag')]
        if not bag_lst:
            print('No bag file found!')
            exit(3)
        
        if not os.path.exists(os.path.join(BAG_DIR, 'csv')):
            os.makedirs(os.path.join(BAG_DIR, 'csv'))
        TARGET_DIR = os.path.join(BAG_DIR, 'csv')
        print('Using bag folder: {}'.format(BAG_DIR))
        print('Using target directory: {}'.format(TARGET_DIR))

    
    config = check_config(config_file)
    if config is None:
        print('Configuration file is invalid!')
        exit(4)
    print('Using config: {}'.format(config_file))

    print('Selected overwrite option: {}'.format(overwrite))
    print('Use -h for help')
    print('Start converting rosbags to csv...')

    for ibag, bag_file in enumerate(bag_lst):
        print(' ({}/{})Start processing:{}'.format(ibag+1, len(bag_lst), bag_file), end='\n')
        for itopic, topic in enumerate(config.keys()):
            print(' ({}/{})Processing topic: {}    \r'.format(itopic+1, len(config.keys()), topic), end='')
            sys.stdout.flush()
            tmp = config[topic]
            FILE_NAME = os.path.join(TARGET_DIR, '.'.join([bag_file, topic]))
            BAG_NAME = os.path.join(BAG_DIR, bag_file)
            if os.path.exists(FILE_NAME):
                if overwrite == 'Always':
                    pass
                elif overwrite == 'Never':
                    continue
                elif overwrite == 'Ask':
                    print('File {} exists! Do you want to overwrite? (y/n) (Default: n)'.format(FILE_NAME))
                    if input() != 'y':
                        print('Skip topic: {}'.format(topic))
                        continue
            with open(FILE_NAME, 'w') as f, rosbag.Bag(BAG_NAME) as bag:
                f.write(','.join(tmp['cols'])+'\n')
                if "arrays" in tmp.keys():
                    for topic, msg, t in bag.read_messages(topics=[tmp['topic']]):
                        for attr in get_nested_attr(msg, tmp["arrays"]["field"]) or []:
                            row = [get_nested_attr(attr, fld) for fld in tmp["fields"][1:]]
                            row.insert(0, t)
                            f.write(','.join(str(i) for i in row))
                            f.write('\n')
                else:
                    for topic, msg, t in bag.read_messages(topics=[tmp['topic']]):
                        row = [get_nested_attr(msg, fld) for fld in tmp["fields"][1:]]
                        row.insert(0, t)
                        f.write(','.join(str(i) for i in row))
                        f.write('\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert rosbag to csv')
    parser.add_argument('dir', type=str, help='Directory of rosbag')
    parser.add_argument('--config_file', '-c', type=str, default=default_config_path, help='Config file')
    parser.add_argument('--overwrite', '-o', type=str, default='Never', choices=['Always', 'Never', 'Ask'], help='Overwrite csv file')

    args = parser.parse_args()
    gen_csv(args.dir, args.config_file, args.overwrite)
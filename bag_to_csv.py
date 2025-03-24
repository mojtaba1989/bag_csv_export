from __future__ import print_function
import os
import json
import rosbag
import numpy as np
import rospy
import sys


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

BAG_DIR = r"/media/moji/Main/ADS/3-19-2025/Loop 3"

bag_lst = [bag for bag in os.listdir(BAG_DIR) if bag.endswith('.bag')]
if not bag_lst:
    print('No bag file found!')
    exit()

if not os.path.exists(os.path.join(BAG_DIR, 'csv')):
    os.makedirs(os.path.join(BAG_DIR, 'csv'))
TARGET_DIR = os.path.join(BAG_DIR, 'csv')

with open("config.json") as json_data_file:
    config = json.load(json_data_file)

for ibag, bag_file in enumerate(bag_lst):
    print(' ({}/{})Start processing:{}'.format(ibag+1, len(bag_lst), bag_file), end='\n')
    for itopic, topic in enumerate(config.keys()):
        print(' ({}/{})Processing topic: {}    \r'.format(itopic+1, len(config.keys()), topic), end='')
        sys.stdout.flush()
        tmp = config[topic]
        FILE_NAME = os.path.join(TARGET_DIR, '.'.join([bag_file, topic]))
        BAG_NAME = os.path.join(BAG_DIR, bag_file)
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
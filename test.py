from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore

import numpy as np
import sys
import argparse
import multiprocessing
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
    
                                                                                     

# Create reader instance and open for reading.
with AnyReader([bagpath], default_typestore=typestore) as reader:
    connections = [x for x in reader.connections if x.topic == '/gps/gps']
    for connection, timestamp, rawdata in reader.messages(connections=connections):
         msg = reader.deserialize(rawdata, connection.msgtype)
         print(timestamp, msg.latitude, msg.longitude, msg.altitude)
         break
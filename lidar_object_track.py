import sys
if sys.version_info < (3, 10):
    raise Exception("Python 3.10 or newer required")

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
import argparse
import time as mTime
import json

def convert_numpy(obj):
    if isinstance(obj, dict):
        return {k: convert_numpy(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy(i) for i in obj]
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    else:
        return obj

class trackObj:
    def __init__(self):
        self.id = None
        self.x = None
        self.y = None
        self.cat = None
        self.time = None
        self.time_register = None
        self.history = []
        self.active = False
        self.matched = False
    
    def add(self, x, y, time):
        self.x = x
        self.y = y
        self.time = time
        self.history.append([x, y, time])
        self.matched = True
    
    def addObj(self, obj):
        self.add(obj.x, obj.y, obj.time)
    
    def dist(self, x, y):
        return self.id, np.sqrt((self.x - x)**2 + (self.y - y)**2)
    
    def distObj(self, obj):
        return self.id, np.sqrt((self.x - obj.x)**2 + (self.y - obj.y)**2)
    
    def plot(self):
        x = np.array([i[0] for i in self.history])
        y = np.array([i[1] for i in self.history])
        plt.scatter(-y, x, s=1, label=f'{self.id}_{self.cat}')

class newObj:
    def __init__(self):
        self.x = None
        self.y = None
        self.time = None
        self.category = None

class newObjList:
    def __init__(self):
        self.newObjList = []
    def add(self, x, y, time, category):
        self.newObjList.append(newObj())
        self.newObjList[-1].x = x
        self.newObjList[-1].y = y
        self.newObjList[-1].time = time
        if 'car' in category:
            self.newObjList[-1].category = 'car'
        elif 'pedestrian' in category:
            self.newObjList[-1].category = 'pedestrian'
        elif 'bike' in category:
            self.newObjList[-1].category = 'bike'
        else:
            self.newObjList[-1].category = 'other'
    
    def remove_FP(self, xlim=3, ylim=2):
        self.newObjList = [obj for obj in self.newObjList if not(abs(obj.x) <= xlim and abs(obj.y) <= ylim)]
    
    def dist(obj1, obj2):
        return np.sqrt((obj1.x - obj2.x)**2 + (obj1.y - obj2.y)**2)
    
    def cleanOther(self, min_dist=2):
        list_no = [obj for obj in self.newObjList if obj.category != 'other']
        list_o = [obj for obj in self.newObjList if obj.category == 'other']
        while len(list_o) > 0:
            obj_t = list_o.pop(0)
            dist = [newObjList.dist(obj_t, obj) for obj in list_no]
            if min(dist) >= min_dist:
                list_no.append(obj_t)
        self.newObjList = list_no
    
    def clean(self, xlim=3, ylim=2, min_dist=2):
        self.remove_FP(xlim, ylim)
        self.cleanOther(min_dist)


def process_lidar_data(CSV_FILE, min_dist=2, max_inactive=2, xlim=3, ylim=2):
    print('Processing lidar data...')
    if not os.path.exists(CSV_FILE):
        print('CSV file not found')
        return
    lidar_data = pd.read_csv(CSV_FILE)
    lidar_data = lidar_data.dropna()
    lidar_data = lidar_data.reset_index(drop=True)

    print('Cleaning lidar data...')
    time = np.unique(lidar_data['time'])
    tracking = []
    archived = []
    id_ = 0
    duration = (time[-1]-time[0])/1e9
    print(f'Recording duration: {duration}s')
    start_time = mTime.time()
    for t in time:
        print(f'{(t-time[0])/1e9:.2f}/{duration:.2f}', end='\r', flush=True)
        for obj in tracking:
            obj.matched = False
            if t - obj.time > max_inactive:
                obj.active = False
        df = lidar_data[lidar_data['time'] == t]
        df = df.reset_index(drop=True)
        new = newObjList()
        for i in range(len(df)):
            new.add(df.loc[i, 'x'], df.loc[i, 'y'], df.loc[i, 'time'], df.loc[i, 'label'])
        new.clean(xlim=xlim, ylim=ylim, min_dist=min_dist)
        for nobj in new.newObjList:
            dist = np.array([obj.distObj(nobj)[1] for obj in tracking if obj.active and not obj.matched])
            dist_id = np.array([obj.distObj(nobj)[0] for obj in tracking if obj.active and not obj.matched])
            if dist.size==0 or dist.min() >= min_dist:
                tracking.append(trackObj())
                tracking[-1].id = id_
                tracking[-1].addObj(nobj)
                tracking[-1].active = True
                tracking[-1].matched = True
                tracking[-1].cat = nobj.category
                tracking[-1].time_register = t
                id_ += 1
            else:
                for idx , obj in enumerate(tracking):
                    if obj.id == dist_id[np.argmin(dist)]:
                        id_target = idx
                        break

                tracking[id_target].addObj(nobj)
                tracking[id_target].matched = True
        for obj in tracking:
            if not obj.active:
                archived.append(obj)
                tracking.remove(obj)
    for obj in tracking:
        archived.append(obj)
    
    print(f'Finished processing lidar data [{mTime.time()-start_time:.2f}s]')
    print(f'Tracked {len(archived)} objects')
    print('Saving into JSON file...')
    jdisct = {}
    for obj in archived:
        jdisct[obj.id] = obj.__dict__
    jdisct = convert_numpy(jdisct)
    with open(CSV_FILE + '.json', 'w') as f:
        json.dump(jdisct, f, indent=4)
    print('Saving into CSV file...')
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process lidar data")
    parser.add_argument("csv", help="Input csv file")
    parser.add_argument("-x", "--x", type=float, help="minimum acceptable x value", default=3)
    parser.add_argument("-y", "--y", type=float, help="minimum acceptable y value", default=2)
    parser.add_argument("-d", "--dist", type=float, help="Minimum distance between objects", default=2)
    parser.add_argument("--inactive-time", type=float, help="Maximum acceptable inactive time", default=2)

    args = parser.parse_args()

    # Process the video
    process_lidar_data(CSV_FILE=args.csv, xlim=args.x, ylim=args.y, min_dist=args.dist, max_inactive=args.inactive_time)


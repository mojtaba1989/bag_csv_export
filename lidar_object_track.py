import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import pandas as pd
import os
import argparse
import time as mTime

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
        self.score = 0
    
    def add(self, x, y, time, score):
        self.x = x
        self.y = y
        self.time = time
        self.history.append([x, y, time])
        self.matched = True
        self.score = np.max([score, self.score])
    
    def addObj(self, obj):
        self.add(obj.x, obj.y, obj.time, obj.score)
    
    def dist(self, x, y):
        return self.id, np.sqrt((self.x - x)**2 + (self.y - y)**2)
    
    def distObj(self, obj):
        return self.id, np.sqrt((self.x - obj.x)**2 + (self.y - obj.y)**2)
    
class newObj:
    def __init__(self):
        self.x = None
        self.y = None
        self.time = None
        self.category = None
        self.score = 0

class newObjList:
    def __init__(self):
        self.newObjList = []
    
    def add(self, x, y, time, category, score):
        self.newObjList.append(newObj())
        self.newObjList[-1].x = x
        self.newObjList[-1].y = y
        self.newObjList[-1].time = time
        self.newObjList[-1].category = category
        self.newObjList[-1].score = score
    
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
    
    def plot(self):
        L = 2.08
        W = 5.82
        rectangle = patches.Rectangle((-L/2, -W/2), L, W, edgecolor='blue', facecolor='blue', linewidth=0, label="Ego Vehicle")
        car = [obj for obj in self.newObjList if obj.category == 'car']
        ped = [obj for obj in self.newObjList if obj.category == 'pedestrian']
        bike = [obj for obj in self.newObjList if obj.category == 'bike']
        other = [obj for obj in self.newObjList if obj.category == 'other']
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot([-obj.y for obj in car], [obj.x for obj in car], 'ro', label='car')
        ax.plot([-obj.y for obj in ped], [obj.x for obj in ped], 'bo', label='pedestrian')
        ax.plot([-obj.y for obj in bike], [obj.x for obj in bike], 'go', label='bike')
        ax.plot([-obj.y for obj in other], [obj.x for obj in other], 'gx', label='other')
        
        ax.add_patch(rectangle)
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=3)
        ax.set_xlim(-60, 60)
        ax.set_ylim(-60, 60)


def process_lidar_data(CSV_FILE, min_dist=2, max_inactive=2, xlim=3, ylim=2):
    print('Processing lidar data...')
    if not os.path.exists(CSV_FILE):
        print('CSV file not found')
        return
    lidar_data = pd.read_csv(CSV_FILE)
    time = []
    for t, nt in zip(lidar_data['time_sec'], lidar_data['time_nsec']):
        time.append(t + nt/1e9)
    lidar_data['time'] = time
    print('Cleaning lidar data...')
    time = np.unique(lidar_data['time'])
    tracking = []
    archived = []
    id_ = 0
    duration = (time[-1]-time[0])
    print(f'Recording duration: {duration}s')
    start_time = mTime.time()
    for t in time:
        print(f'{(t-time[0]):.2f}/{duration:.2f}', end='\r', flush=True)
        for obj in tracking:
            obj.matched = False
            if t - obj.time > max_inactive:
                obj.active = False
        df = lidar_data[lidar_data['time'] == t]
        df = df.reset_index(drop=True)
        new = newObjList()
        for i in range(len(df)):
            new.add(df.loc[i, 'x'], df.loc[i, 'y'], df.loc[i, 'time'], df.loc[i, 'label'], df.loc[i, 'score'])
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
    print('Saving into CSV file...')
    with open(CSV_FILE + '.processed', 'w') as f:
        f.write('time_sec,time_nsec,id,x,y,z,label,score,time\n')
        for idx, obj in enumerate(archived):
            print(f'write {idx}/{len(archived)}', end='\r', flush=True)
            for h in obj.history:
                f.write(f'{int(h[2])},{int((h[2]-int(h[2]))*1e9)},{obj.id},{h[0]},{h[1]},-1,{obj.cat},{obj.score},{h[2]}\n')
    print('Sorting and re-indexing csv file...')
    data = pd.read_csv(CSV_FILE+'.process')
    data = data.sort_values(['time_sec', 'time_nsec'])
    data.to_csv(CSV_FILE+'.process', index=False)
    print(f'Result saved to {CSV_FILE}.processed')
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


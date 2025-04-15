import os
import json
import rosbag
import rospy
import sys

def get_topics(BAG_DIR):
    if not os.path.exists(os.path.join(BAG_DIR, 'topics')):
        os.makedirs(os.path.join(BAG_DIR, 'topics'))
    bag_lst = [bag for bag in os.listdir(BAG_DIR) if bag.endswith('.bag')]
    BAG_FILE = BAG_NAME = os.path.join(BAG_DIR, bag_lst[0])
    TARGET_DIR = os.path.join(BAG_DIR, 'topics')

    topics = set()
    with rosbag.Bag(BAG_NAME) as bag:
        for topic, msg, t in bag.read_messages():
            if topic in topics:
                continue
            FILE_NAME = topic.replace('/','.')
            if FILE_NAME[0]=='.':
                FILE_NAME = FILE_NAME[1:]
            FILE_NAME = os.path.join(TARGET_DIR, FILE_NAME)
            with open(FILE_NAME, 'w') as f:
                f.write(topic+'\n')
                f.write(str(msg)+'\n')
            topics.add(topic)
    with open(os.path.join(TARGET_DIR, 'topics.txt'), 'w') as f:
        f.write('\n'.join(topics))
if __name__ == '__main__':
    BAG_DIR = rospy.myargv(argv=sys.argv)[1]
    get_topics(BAG_DIR)


            

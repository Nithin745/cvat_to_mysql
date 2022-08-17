from copy import deepcopy, copy
from datetime import datetime
from time import sleep
from collections import Counter
import json
import string
import os
from get_json import main as download_json
from label_map import action_map
import pymongo
import re
import sys


words = string.ascii_lowercase

src_folder = os.path.join(os.getcwd(), 'files')
dest_path = os.path.join(os.getcwd(), 'result')
TASK_URL = os.environ.get('BASE_URL') + 'tasks/'


def connect_db():
    client = pymongo.MongoClient(
        "mongodb+srv://Pradeep:ShopperAI@israel.dien7.mongodb.net/?retryWrites=true&w=majority")
    db = client['victory']

    return db


def get_last_entry_date(tasks):
    date = tasks.find({}).sort('completed_date', -1)
    # print(date[0])
    try:
        last_date = date[0]['completed_date']
    except:
        return None
    
    return last_date.split('T')[0]


def clean_dir():
    files = [item for item in os.listdir(src_folder) if item.endswith('.json')]
    result = [item for item in os.listdir(dest_path) if item.endswith('.json')]
    if files:
        for item in files:
            os.remove(os.path.join(src_folder, item))
    if result:
        for item in result:
            os.remove(os.path.join(dest_path, item))

def delete_entries():
    db = connect_db()
    db['rishon_lezion_71_planograms'].delete_many({})
    db['rishon_lezion_71_pg_tasks'].delete_many({})


class PrepareJson:
    """
    This module prepares the json file that is downloaded from cvat and merge images and their annotations in 
    a single json object by making some changes to the json file, So, that it is easy to upload it to MySql
    """

    def __init__(self, path, file, dest_path):
        with open(path, 'r') as f:
            self.data = json.load(f)
        self.file = file
        self.dest_path = dest_path
        self.db_task_ids = []
        self.add_category_name()
        self.add_task_id()
        self.add_camera_id_field()

    def extract_data(self,):
        tasks = self.data['tasks']
        tasks_length = len(tasks)
        for idx in range(tasks_length):
            image = [img for img in self.data['images']
                     if img['task_id'] == tasks[idx]['task_id']]
            if image:
                task, images = self._split_data(tasks, idx, image)

            else:
                continue

    def _split_data(self, tasks, idx, images):
        task = tasks[idx]
        img_ids = [ids['id'] for ids in images]
        annotations = [item for item in self.data['annotations']
                       if item['image_id'] in img_ids]
        images = self.merge_annotations(images, annotations)

        return task, images

    def merge_annotations(self, images, annotations):
        for img in images:
            match = []
            for item in annotations:
                if item['image_id'] == img['id']:
                    match.append(item)
                    continue
            if match:
                img['annotations'] = match
            # else:
            #     img['annotations'] = 'none'

        return images

    def add_task_id(self):
        task_ids = [task['id'] for task in self.data['tasks']]
        for img in self.data['images']:
            if img['task_id'] in task_ids:
                img['task_id'] = self.get_task_id(img, task_ids)

    def get_task_id(self, img, task_ids):
        task_id = [x for x in task_ids if x == img['task_id']]
        task_id = task_id[0]

        for item in self.data['tasks']:
            if item['id'] == task_id:
                task_pk = item['task_id']

        return task_pk

    def add_category_name(self):
        category = self.data['categories']
        category_ids = [ids['id'] for ids in category]
        for item in self.data['annotations']:
            if int(item['category_id']) in category_ids:
                name = self.get_category_name(item, category_ids, category)
                item['category_name'] = name
                del item['category_id']

    def get_category_name(self, annotation, category_ids, category):
        category_id = [x for x in category_ids if x == int(annotation['category_id'])]
        category_id = int(category_id[0])
        for item in category:
            if item['id'] == category_id:
                category_name = item['name']
                break
            else:
                category_name = 'null'

        return category_name

    def delete_junks(self):
        keys = ['tasks', 'images']
        for key in self.data.copy():
            if key not in keys:
                del self.data[key]
        for key in keys:
            for item in self.data[key]:
                if key == 'tasks':
                    del item['id']
                if key == 'images':
                    del item['id']
                    del item['video']

    def save_json(self):
        self.delete_junks()
        dest_path = os.path.join(self.dest_path, self.file)
        with open(dest_path, 'w', encoding='utf-8') as F:
            json.dump(self.data, F, ensure_ascii=False, indent=4)

        return self.file

    def add_camera_id_field(self, pattern='camera[0-9]+'):
        for obj in self.data['tasks']:
            # camera_id_idxs = (re.search('camera', obj['video'].lower()).span())
            filename = obj['video']
            if 'planogram' in obj['name']:
                filename = obj['name']
            try:
                camera_id_str = re.findall(pattern, filename.lower())[0]
                camera_id = camera_id_str[len("camera"):]
                obj['camera_id'] = camera_id
            except:
                print("Warning - Cannot find camera number in video {}".format(obj['video']))


class PushToMongoDb:
    """
    This module preprocess the merged json file before pushing it to the MySql, like generating
    video_id, buery, actions etc.,
    """

    def __init__(self, path):
        with open(path, 'r') as F:
            self.data = json.load(F)
        self.tasks = self.data['tasks']
        self.images = self.data['images']
        self.db = connect_db()
        self.category = ['actions', 'action']
        self.retailer_id = 1
        self.branch_id = 71
        self.default = '-'
        self.empty = ['0', '-']
        self.gt, self.planagram = [], []
        self.check_previous_entries()

    def check_previous_entries(self):
        planagram = self.db['rishon_lezion_71_planograms'].find({}, projection ={'task_id':True})
        gt_list = []
        for item in planagram:
            self.planagram.append(item['task_id'])

        gt = self.db['rishon_lezion_71_gt'].find({}, projection ={'task_id':True})
        for gt_item in gt:
            try:
                gt_list.append(gt_item['task_id'])
            except KeyError:
                continue
        self.gt = list(set(gt_list))
        # print(self.gt)

    def get_camera_name(self, filename, planogram, pattern='camera[0-9]+'):
        """This return the camera name and camera number"""
        camera_id = False
        if planogram:
            filename = filename.lstrip('planogram_')
        try:
            camera_id_str = re.findall(pattern, filename.lower())[0]
            camera_id = camera_id_str[len("camera"):]
        except:
            print("Workning - Cannot find camera number in video {}".format(filename))

        return camera_id_str, camera_id

    def get_actions_buyer(self, actions):
        """This function generates action id for each buyer"""
        actions_list = []
        buyer = []
        for i, action in enumerate(actions):
            act = list(action)
            buyer.append(act[1])
            act_id = dict(Counter(buyer))
            act[2] = act_id[act[1]]
            act_tuple = tuple(act)
            actions_list.append(act_tuple)

        return actions_list

    def push_to_mongo(self):
        """
        This is the main function where we push the data to MongoDB
        """
        buyer_table = []
        planogram_coll = []
        pg_tasks = []
        for task in self.tasks:
            person = []
            planogram = None
            planogram_flag = False
            # Filtering frames that belongs to current task_id
            imgs = [i for i in self.images if task['task_id'] == i['task_id']]
            if 'planogram' in task['name']:
                planogram_flag = True
                filename = task['name']
            else:
                filename = task['video']
            for img in imgs:  # Loop through the filtered images
                sec = self.get_sec(img['file_name'])  # This gets the seconds for the current frame
                frame_no = self.get_frame_no(img['file_name'])
                # Get data for all the tables video, buyer, actions
                if 'planogram' in task['name']:
                    pg_tasks.append(task)
                    planogram_flag = True
                    filename = task['name']
                    camera_id_str, camera = self.get_camera_name(filename, False)
                    date, filename = self.get_filename(filename, camera_id_str, True)
                    # print(filename)
                    img['name'] = filename
                    img['completed_date'] = task['completed_date']
                    image = self.build_planogram(img)
                    planogram_coll.append(image.copy())
                    # self.db['rishon_lezion_71_planograms'].insert_one(image)
                else:
                    filename = task['video']
                    buyer = self.get_video_data(
                        img, filename, sec, task['task_id'], frame_no, planogram_flag)
                    if buyer:
                        for buy in buyer:
                            buy['completed_date'] = task['completed_date']
                            buyer_table.append(buy.copy())
                            if buy['buyer_id'] not in person:
                                person.append(buy['buyer_id'])

        if buyer_table:  # If buyer data and action is not empty we are ready to push it to MongoDB
            final_buyer = self.build_collection(buyer_table, person, task['task_id'])
            match = self.check_for_match(final_buyer, planogram_flag)
            print(f"Buyer Length: {len(final_buyer)}")
            # if match:
            #     self.update_data(final_buyer, match, planogram_flag)
            # else:
            #     self.insert_new(final_buyer, planogram_flag)
            # print(task['task_id'])
            # print(final_buyer)
        if planogram_coll:
            # print(planogram_coll)
            match = self.check_for_match(planogram_coll, planogram_flag)
            print(f"Planogram length: {len(planogram_coll)} {len(pg_tasks)}")
            if match:
                self.update_data(planogram_coll, match, planogram_flag, pg_tasks)
            else:
                self.insert_new(planogram_coll, planogram_flag, pg_tasks)

    def check_for_match(self, data, planagram):
        if planagram:
            match = [item['task_id'] for item in data if item['task_id'] in self.planagram]
        else:
            match = [item['task_id'] for item in data if item['task_id'] in self.gt]

        return match

    def update_data(self, records, match, planagram, tasks=None):
        if planagram:
            delete = self.db['rishon_lezion_71_planograms'].delete_many({
                'taks_id': {'$in': match}
            })
            delete_task = seld.db['rishon_lezion_71_pg_tasks']. delete_many({
                'taks_id': {'$in': match}
                })
            insert = self.db['rishon_lezion_71_planograms'].insert_many(tasks)
            task_insert = self.db['rishon_lezion_71_pg_tasks'].insert_many(tasks)
        else:
            delete = self.db['rishon_lezion_71_gt'].delete_many({
                'taks_id': {'$in': match}
            })
            insert = self.db['rishon_lezion_71_gt'].insert_many(records)
            print(insert.inserted_ids)

    def insert_new(self, records, planagram, pg_tasks):
        if planagram:
            insert = self.db['rishon_lezion_71_planograms'].insert_many(records)
            insert = self.db['rishon_lezion_71_pg_tasks'].insert_many(pg_tasks)
        else:
            insert = self.db['rishon_lezion_71_gt'].insert_many(records)
            print(insert.inserted_ids)

    def build_planogram(self, images: dict) -> dict:
        del images['file_name']
        for item in images['annotations']:
            del item['id']
            del item['image_id']

        # print(images)
        return images

    def build_collection(self, buyers, persons, task_id):
        buyers_list = []
        for i in persons:
            buyer_list = [item for item in buyers if item['buyer_id'] == i]
            age, gender, filename = self.get_age(buyer_list)
            # print(age, gender)
            action = self.build_buyer(buyer_list)
            buyer_dict = {
                'age': age,
                'gender': gender,
                'buyer_id': filename + '_' + i,
                'actions': action,
                'task_id': task_id
            }
            buyers_list.append(buyer_dict.copy())
        with open(filename + '.json', 'w') as F:
            json.dump(buyers_list, F, indent=4)

        return buyers_list

    def build_buyer(self, buyers):
        actions = []
        for buyer in buyers:
            act_dict = {
                'name': action_map[buyer['action']],
                'second_in_video': buyer['sec'],
                'frame_index': buyer['frame_no'],
                'bbox': buyer['bbox'],
            }
            actions.append(act_dict.copy())

        return actions

    def get_age(self, buyers):
        # age, gender = None, None
        for buyer in buyers:
            age, gender = buyer['age'], buyer['gender']
            if age not in self.empty and gender not in self.empty:
                break
            elif age in self.empty and gender not in self.empty:
                break
            elif age not in self.empty and gender in self.empty:
                break
            elif age in self.empty and gender in self.empty:
                continue

        return age, gender, buyer['filename']

    def get_sec(self, frame, sec=.5):
        """This method returns seconds for the given frame"""
        frame = frame.rstrip('.PNG')
        frame = int(frame.split('_')[1]) / 10
        # print(frame, f"{(frame * sec):.1f}")
        return f"{(frame * sec):.1f}"

    def get_frame_no(self, frame):
        """This method returns seconds for the given frame"""
        frame = frame.rstrip('.PNG')

        return int(frame.split('_')[1]) / 10

    def get_video_data(self, images, filename, sec, task_id, frame_no, planogram):
        """This method extracts buyer with `id`, `action` and `hand` lables"""
        buyer_data = []
        camera_id_str, camera = self.get_camera_name(filename, planogram)
        if 'annotations' in images:
            date, filename = self.get_filename(filename, camera_id_str, planogram)
            buyer, action = None, None
            if camera and images['annotations'] != 'none':
                for annotation in images['annotations']:
                    if annotation['category_name'] == 'actions' and annotation['attributes'] is not None:
                        if 'person_id' in annotation['attributes'] and 'action' in annotation['attributes']:
                            age, gender, product = self.default, self.default, self.default
                            person_id, action = annotation['attributes']['person_id'], annotation['attributes']['action']
                            if 'demographic_age' in annotation['attributes']:
                                age = annotation['attributes']['demographic_age']
                            if 'demographic_gender' in annotation['attributes']:
                                gender = annotation['attributes']['demographic_gender']
                            # if 'product' in annotation['attributes']:
                            #     product = annotation['attributes']['product']
                            if action == 'ts':
                                action = 'hts'
                            elif action == 'h':
                                action = 'hh'
                            buyer = {
                                # 'task_id': task_id,
                                'filename': filename,
                                'buyer_id': person_id,
                                'age': age,
                                'gender': gender,
                                'action': action,
                                'sec': sec,
                                'frame_no': frame_no,
                                'bbox': annotation['bbox']
                            }
                            if person_id == '' or person_id in words:
                                buyer = None
                    elif annotation['category_name'] == 'hands' and annotation['attributes'] is not None:
                        if 'person_id' in annotation['attributes'] and 'hand' in annotation['attributes']:
                            age, gender, product = self.default, self.default, self.default
                            person_id, hand = annotation['attributes']['person_id'], annotation['attributes']['hand']
                            if 'demographic_age' in annotation['attributes']:
                                age = annotation['attributes']['demographic_age']
                            if 'demographic_gender' in annotation['attributes']:
                                gender = annotation['attributes']['demographic_gender']
                            # if 'product' in annotation['attributes']:
                            #     product = annotation['attributes']['product']
                            if hand == 'ts':
                                hand = 'hts'
                            elif hand == 'h':
                                hand = 'hh'
                            buyer = {
                                # 'task_id': task_id,
                                'filename': filename,
                                'buyer_id': person_id,
                                'age': age,
                                'gender': gender,
                                'action': hand,
                                'sec': sec,
                                'frame_no': frame_no,
                                'bbox': annotation['bbox']
                            }
                            if person_id == '' or person_id in words:
                                buyer = None
                    if buyer:
                        buyer_data.append(buyer)
                        buyer = None
                    else:
                        continue

        return buyer_data

    def get_filename(self, filename: str, camera, planogram):
        """This method generates filename for the video_id column"""
        if planogram:
            filename = filename.lstrip('planogram_')
            date_time = '_'.join(filename.split('_', 2)[:2])
            return date_time, date_time + '_' + camera
        date_time = '_'.join(filename.split('_', 2)[:2])
        return date_time, date_time + f'_{self.retailer_id}_{self.branch_id}_' + camera


def main():
    """This is the main function that calls all the modules to downloa all the json file,
    process it and  push it to MySql
    """
    db = connect_db()
    date = get_last_entry_date(db['rishon_lezion_71_planograms'])
    if date:
        download_json(date=date)
    download_json()
    if any(os.scandir(src_folder)):
        for file in os.listdir(src_folder):
            path = os.path.join(src_folder, file)
            prepare_data = PrepareJson(path, file, dest_path)
            prepare_data.extract_data()
            cleanned_file = prepare_data.save_json()
            target_path = os.path.join(dest_path, cleanned_file)
            print(f"Path: {target_path}")
            mongo_data = PushToMongoDb(target_path)
            mongo_data.push_to_mongo()
        clean_dir()


if __name__ == '__main__':
    main()
    # delete_entries()
    # target_path = os.path.join(dest_path, 'Shir.json')
    # mongo_data = PushToMongoDb(target_path)
    # mongo_data.push_to_mongo()
    # log = {'updated_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    # with open('log1.json', 'w') as F:
    #     json.dump(log, F, indent=4)

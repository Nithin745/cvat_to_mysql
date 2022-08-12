from datetime import datetime
from time import sleep
from collections import Counter
import json
import string
import os
import mysql.connector as sql
from get_json import main as download_json
from get_json import _header
import re
import sys
import requests


words = string.ascii_lowercase

src_folder = os.path.join(os.getcwd(), 'files')
dest_path = os.path.join(os.getcwd(), 'result')
TASK_URL = os.environ.get('BASE_URL') + 'tasks/'


def connect_db():
    database = sql.connect(
        host="3.67.247.142",
        user="root",
        passwd="Nj375#Dz9ebfNmJ%$7&",
        database="test_1",
    )
    cursor = database.cursor()
    return database, cursor


def inject_to_sql(query, data, many=None, kind=None):
    mydb, my_cursor = connect_db()
    if many:
        my_cursor.executemany(query, data)
    else:
        my_cursor.execute(query, data)
    if kind == 'select':
        last_id = my_cursor.fetchone()
    else:
        mydb.commit()
        last_id = my_cursor.lastrowid

    return last_id


def execute_sql_query(query):
    mydb, my_cursor = connect_db()

    my_cursor = mydb.cursor()
    my_cursor.execute(query)
    return my_cursor.fetchall()


def get_last_entry_date(tasks):
    date = tasks.find({}).sort('completed_date', -1)
    print(date[0])
    last_date = date[0]['completed_date']

    return last_date.split('T')[0]


def mark_as_processed(task_id):
    # with open('processed_id.json', 'r') as F:
    #     data = json.load(F)
    # task_ids = data['task_id']
    headers = _header()
    payload = {"processed: True"}
    url = TASK_URL + str(task_id)
    res = requests.patch(url=url, json=payload, headers=headers)
    if res.ok:
        print(f"Marked task {task_id} as processed")
    else:
        print(f"Error while marking task as processed {res.text}")


def delete_entries(task_id):
    tup = (task_id, )
    video_id = inject_to_sql("SELECT video_id FROM video where task_id =%s", tup, kind='select')
    video_id = ''.join(video_id)

    video_query = inject_to_sql(f"delete from video where task_id=%s", tup)
    actions_query = inject_to_sql(f"delete from actions where video_id=%s", (video_id,))
    buyer_query = inject_to_sql(f"delete from buyer where video_id=%s", (video_id,))


def get_task_ids():
    task_ids = execute_sql_query("SELECT task_id FROM video")
    # task_ids = task_ids.fetchall()
    task_ids = [item for tup in task_ids for item in tup]
    print(f"Got all task ids from database {len(task_ids)}")

    return task_ids


task_ids = get_task_ids()


def clean_dir():
    files = [item for item in os.listdir(src_folder) if item.endswith('.json')]
    result = [item for item in os.listdir(dest_path) if item.endswith('.json')]
    if files:
        for item in files:
            os.remove(os.path.join(src_folder, item))
    if result:
        for item in result:
            os.remove(os.path.join(dest_path, item))


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
            try:
                camera_id_str = re.findall(pattern, obj['video'].lower())[0]
                camera_id = camera_id_str[len("camera"):]
                obj['camera_id'] = camera_id
            except:
                print("Worning - Cannot find camera number in video {}".format(obj['video']))


class PushToMySql:
    """
    This module preprocess the merged json file before pushing it to the MySql, like generating
    video_id, buery, actions etc.,
    """

    def __init__(self, path):
        with open(path, 'r') as F:
            self.data = json.load(F)
        self.tasks = self.data['tasks']
        self.images = self.data['images']
        self.dbtask_ids = []
        self.category = ['actions', 'action']
        self.retailer_id = 1
        self.branch_id = 71
        self.default = '-'
        self.empty = ['0', '-']

    def get_camera_name(self, filename, pattern='camera[0-9]+'):
        """This return the camera name and camera number"""
        camera_id = False
        try:
            camera_id_str = re.findall(pattern, filename.lower())[0]
            camera_id = camera_id_str[len("camera"):]
            # print(camera_id_str)
        except:
            print("Worning - Cannot find camera number in video {}".format(filename))

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

    def push_to_sql(self):
        """
        This is the main function where we push the data to MySql
        """
        for task in self.tasks:
            video_table = []
            action_table = []
            buyer_table = []
            person = []
            # Filtering frames that belongs to current task_id
            imgs = [i for i in self.images if task['task_id'] == i['task_id']]
            for img in imgs:  # Loop through the filtered images
                sec = self.get_sec(img['file_name'])  # This gets the seconds for the current frame
                # Get data for all the tables video, buyer, actions
                video, buyer, action = self.get_video_data(img, task['video'], sec, task['task_id'])
                if buyer:
                    for buy in buyer:
                        f_name, person_id, age, gender = buy
                        buyer_table.append(buy)
                        person.append(person_id)
                if video and len(video_table) == 0:
                    video_table.append(video)
                if action:
                    for act in action:
                        action_table.append(act)

            if buyer_table and action_table:  # If buyer data and action is not empty we are ready to push it to MySql
                if task['task_id'] in task_ids:
                    delete_entries(task['task_id'])
                    print(f"Deleted entries for {task['task_id']}")
                _person = list(set(person))
                _buyer = list(set(buyer_table))
                final_buyer = self.filter_buyer(_person, _buyer)

                try:
                    inject_to_sql("INSERT INTO video (video_id, date, retailer_id, branch_id, camera, task_id) VALUES \
                            (%s, %s, %s, %s, %s, %s)", video_table, True)
                except Exception as e:
                    print(f"Video Error: {video_table}")
                    print(e)
                    # sys.exit()
                actions = self.get_actions_buyer(action_table)
                try:
                    inject_to_sql("INSERT INTO buyer (video_id, buyer_id, age, gender) VALUES \
                            (%s, %s, %s, %s)", final_buyer, True)
                    print(f"Inserting Buyer: {final_buyer}")
                except Exception as e:
                    print(f"Buyer Error: {final_buyer}")
                    print(e)
                    # sys.exit()
                if actions:
                    print(f"actions task: {task['task_id']}")
                    actions.sort(key=lambda y: y[-1])
                    try:
                        inject_to_sql("INSERT INTO actions (action, buyer_id, action_id, video_id, product, sec) VALUES \
                                (%s, %s, %s, %s, %s, %s)", actions, True)
                        print(f"Inserting Action: {actions}")
                        print('*'*100)
                    except Exception as e:
                        print(f"Action Error: {actions}")
                        print(e)
                        # sys.exit()
                    mark_as_processed(task['task_id'])

    def filter_buyer(self, person, buyer):
        """
        This function look for buyer with both age and gender, if it doesn't find that, it will 
        the buyer without age and gender
        """
        final_buyer = []
        for i in person:
            per = None
            for j in buyer:
                file, _person_id, _age, _gender = j
                if i == _person_id and _gender not in self.empty and _age not in self.empty:
                    per = j
                    break
                elif i == _person_id and _gender in self.empty and _age not in self.empty:
                    per = j
                    break
                elif i == _person_id and _gender not in self.empty and _age in self.empty:
                    per = j
                    break
                elif i == _person_id and _gender in self.empty and _age in self.empty:
                    per = j
            if per:
                final_buyer.append(per)

        return final_buyer

    def get_sec(self, frame, sec=.5):
        """This method returns seconds for the given frame"""
        frame = frame.rstrip('.PNG')
        frame = int(frame.split('_')[1]) / 10
        # print(frame, f"{(frame * sec):.1f}")
        return f"{(frame * sec):.1f}"

    def get_video_data(self, images, filename, sec, task_id):
        """This method extracts buyer with `id`, `action` and `hand` lables"""
        video_data, buyer_data, action_data = False, [], []
        camera_id_str, camera = self.get_camera_name(filename)
        if 'annotations' in images:
            date, filename = self.get_filename(filename, camera_id_str)
            time = datetime.strptime(date, '%Y-%m-%d_%H-%M-%S')
            video_data = (filename, time, self.retailer_id, self.branch_id, camera, task_id)
            buyer, action = None, None
            # print(f"Current task id: {task_id}")
            if camera and images['annotations'] != 'none':
                for annotation in images['annotations']:
                    if annotation['category_name'] == 'actions' and annotation['attributes'] is not None:
                        if 'person_id' in annotation['attributes'] and 'action' in annotation['attributes']:
                            # print(f"Task in action: {task_id}")
                            # if not task_id in tasks_id:
                            #     tasks_id.append(task_id)
                            age, gender, product = self.default, self.default, self.default
                            person_id, action = annotation['attributes']['person_id'], annotation['attributes']['action']
                            if 'demographic_age' in annotation['attributes']:
                                age = annotation['attributes']['demographic_age']
                            if 'demographic_gender' in annotation['attributes']:
                                gender = annotation['attributes']['demographic_gender']
                            if 'product' in annotation['attributes']:
                                product = annotation['attributes']['product']
                            if action == 'ts':
                                action = 'hts'
                            elif action == 'h':
                                action = 'hh'
                            buyer = (filename, person_id, age, gender)
                            action = (action, person_id, 0, filename, product, sec)
                            if person_id == '' or person_id in words:
                                buyer = None
                                action = None
                    elif annotation['category_name'] == 'hands' and annotation['attributes'] is not None:
                        if 'person_id' in annotation['attributes'] and 'hand' in annotation['attributes']:
                            # print(f"Task in hand: {task_id}")
                            # if not task_id in tasks_id:
                            #     tasks_id.append(task_id)
                            age, gender, product = self.default, self.default, self.default
                            person_id, hand = annotation['attributes']['person_id'], annotation['attributes']['hand']
                            if 'demographic_age' in annotation['attributes']:
                                age = annotation['attributes']['demographic_age']
                            if 'demographic_gender' in annotation['attributes']:
                                gender = annotation['attributes']['demographic_gender']
                            if 'product' in annotation['attributes']:
                                product = annotation['attributes']['product']
                            if hand == 'ts':
                                hand = 'hts'
                            elif hand == 'h':
                                hand = 'hh'
                            buyer = (filename, person_id, age, gender)
                            action = (hand, person_id, 0, filename, product, sec)
                            if person_id == '' or person_id in words:
                                buyer = None
                                action = None
                    if buyer:
                        buyer_data.append(buyer)
                        buyer = None
                    if action:
                        action_data.append(action)
                        action = None
                    else:
                        continue

        return video_data, buyer_data, action_data

    def get_filename(self, filename, camera):
        """This method generates filename for the video_id column"""
        date_time = '_'.join(filename.split('_', 2)[:2])
        return date_time, date_time + f'_{self.retailer_id}_{self.branch_id}_' + camera


def main():
    """This is the main function that calls all the modules to downloa all the json file,
    process it and  push it to MySql
    """
    camera = ['camera5', 'camera6', 'camera7']
    for cam in camera:
        download_json(cam)
        if any(os.scandir(src_folder)):
            for file in os.listdir(src_folder):
                path = os.path.join(src_folder, file)
                prepare_data = PrepareJson(path, file, dest_path)
                prepare_data.extract_data()
                cleanned_file = prepare_data.save_json()
                target_path = os.path.join(dest_path, cleanned_file)
                mongo_data = PushToMySql(target_path)
                mongo_data.push_to_sql()
            clean_dir()


if __name__ == '__main__':
    main()
    # log = {'updated_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    # with open('log1.json', 'w') as F:
    #     json.dump(log, F, indent=4)

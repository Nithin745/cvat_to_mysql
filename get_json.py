import requests
import json
import pandas as pd
from urllib import parse
import os
from datetime import datetime
from pytz import utc
from dotenv import load_dotenv

load_dotenv()

filename = 'admin_data.csv'
login_data = pd.read_csv(filename, na_values=True)

tasks_url = os.environ.get('BASE_URL') + 'tasks'
projects = os.environ.get('BASE_URL') + 'projects'
login_url = os.environ.get('BASE_URL') + 'auth/login'
json_url = os.environ.get('DOWNLOAD_URL')

project_dir = os.path.join(os.getcwd(), 'files')
processed_id = {'tasks_id': []}


def get_members_list(admin_data):
    """
    Get annotators details like name, id, url and last upload name.
    """
    header = _header(admin_data)  # Using helper function to get header for the request
    payload = {
        'page_size': 20,
        'page': 1,
        'names_only': 'true',
    }
    with requests.session() as S:
        user = S.get(url=projects, headers=header, params=payload)

    if user.ok:
        user_list = user.text
        user_list = json.loads(user_list)
        user_list = user_list['results']
        col_names = ['id', 'name']
        user_df = pd.DataFrame(user_list)
        user_df = user_df[col_names]
        user_df.sort_values('id', inplace=True, ignore_index=True)
        user_df['updated_on'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        user_df.to_csv('user_info.csv', index_label='index')
        print("ok")
    else:
        fresh_admin_data = login(admin_data, filename)
        get_members_list(fresh_admin_data)


def login(admin_data, filename):
    """
    This function login with given username and password, only if there are no 
    tokens ans sessionid or if session id is expired, this function is only 
    called only if any of the above mentioned conditions are true.
    """
    with requests.session() as s:
        username = admin_data['username'].item()
        password = admin_data['password'].item()
        cred = {
            'username': username,
            'password': password
        }
        login_header = {
            'Accept': 'application/json, text/plain, */*',
            'Authorization': '',
            'Connection': 'keep-alive',
            'X-CSRFTOKEN': ''
        }
        res = s.post(url=login_url, json=cred, headers=login_header)
        if res.status_code == 200:
            token = res.text
            token = json.loads(token)
            cookie = res.cookies.get_dict()
            admin_data['csrftoken'] = cookie['csrftoken']
            admin_data['sessionid'] = cookie['sessionid']
            admin_data['token'] = token['key']
            admin_data['updated_on'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            admin_data.to_csv(filename, index=False)
        else:
            print('error')

    return admin_data


def _header(admin_data=login_data, for_json=False):
    """
    This helper function has only one task, return header when called
    """
    if for_json:
        header = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-GPC': '1'
        }
    else:
        csrf = admin_data['csrftoken'].item()
        token = admin_data['token'].item()
        sessionid = admin_data['sessionid'].item()
        header = {
            'Accept': 'application/json, text/plain, */*',
            'Authorization': f'Token {token}',
            'Cookie': f'csrftoken={csrf}; sessionid={sessionid}',
            'Connection': 'keep-alive',
            'Sec-GPC': '1',
            'X-CSRFTOKEN': str(csrf),
        }

    return header


def download_json(user_df, name, project_id, ids, from_date, to_date):
    task_ids = ",".join(ids)
    header = _header(user_df, True)
    if from_date:
        payload = {
            'tasks_id': task_ids,
            'project_id': project_id,
            'status': 'completed',
            'fromDate': from_date,
            'toDate': to_date
        }
    else:
        payload = {
            'tasks_id': task_ids,
            'project_id': project_id,
            'status': 'completed',
            'fromDate': '01-06-2021',
            'toDate': to_date
        }
    with requests.session() as S:
        json_data = S.get(url=json_url, headers=header, params=payload)
    size = len(json_data.text)
    if json_data.ok and size > 0:
        data = json.loads(json_data.text)
        print(f'Downloaded file for {name}')
        _fix_and_save(data, name)
    else:
        print('failed', json_data.url)
        print(json_data.text)


def _fix_and_save(json_data, name):
    """
    This function adds task_id to all the annotation items and 
    save to json file
    """
    path = os.path.join(project_dir, name+'.json')
    tasks = json_data['tasks']
    images = json_data['images']
    annotation = json_data['annotations']
    for item in annotation:
        image_id = item['image_id']
        image_id -= 1
        task_id = images[image_id]['task_id']
        task_id -= 1
        id = tasks[task_id]['task_id']
        item['task_id'] = id

    with open(path, 'w', encoding='utf-8') as j:
        json.dump(json_data, j, ensure_ascii=False, indent=4)
    for obj in tasks:
        tk_id = obj['task_id']
        processed_id['tasks_id'].append(tk_id)


def generate_filter_query(project_id, to_date, from_date=None, status="completed", camera=None):
    query = {
        "and": [
            {
                "==": [
                    {
                        "var": "project_id"
                    },
                    int(project_id)
                ]
            },
            {
                "==": [
                    {
                        "var": "status"
                    },
                    status
                ]
            },
            {
                "==": [
                    {
                        "var": "processed"
                    },
                    False
                ]
            },
            {
                "==": [{"var": "camera"}, camera]
            }
        ]
    }
    if from_date:
        for item in query['and']:
            for key in item.keys():
                if key == "<=":
                    item[key].insert(0, from_date)
    query = json.dumps(query)
    return query


def get_task_ids(admin_data, date_after=None, cam=None):
    user_df = pd.read_csv('user_info.csv')
    header = _header(admin_data)
    idx = user_df['index']
    skip_id = [3, 8]
    idx = [x for x in idx if x not in skip_id]
    date = datetime.now(utc)
    to_date = date.strftime('%d-%m-%Y')
    if date_after:
        date_raw = datetime.strptime(date_after, '%Y-%m-%d')
        after = date_raw.strftime('%Y-%m-%d')
        from_date = date_raw.strftime('%d-%m-%Y')
        print(after, from_date)
    else:
        from_date = None
    for i in idx:
        ids = []
        project_id = user_df.loc[i, 'id']
        name = user_df.loc[i, 'name']
        print(name)
        page = 1
        if date_after:
            payload = {
                "filter": generate_filter_query(project_id, date.strftime('%Y-%m-%d'), after, camera=cam)
            }
        else:
            payload = {
                'filter': generate_filter_query(project_id, date.strftime('%Y-%m-%d'), camera=cam)
            }
            print(f"payload===> : {payload}")
        payload.update({'page_size': 250})
        while True:
            if page > 1:
                payload['page'] = page

            with requests.Session() as S:
                res = S.get(tasks_url, headers=header, params=payload)
                print(res.url)
            if res.ok:
                data = res.json()
                results = data['results']
                next_page = data['next']
                page += 1
                for task in results:
                    ids.append(str(task['id']))
                if not next_page:
                    break
            else:
                break
        if ids:
            print(ids)
            download_json(user_df, name, project_id, ids, from_date, to_date)
        else:
            print(f"No new entries for: {name}")


def main(cam, date=None):
    # filename = 'admin_data.csv'
    admin_data = pd.read_csv(filename, na_values=True)
    col_name = ['csrftoken', 'token', 'sessionid']
    col_exist = set(col_name).issubset(admin_data.columns)
    if col_exist:
        csrf = admin_data['csrftoken']
        token = admin_data['token']
        if csrf.any() and token.any():
            get_members_list(admin_data)
            get_task_ids(admin_data, date, cam)
        else:
            admin_data = login(admin_data, filename)
            get_members_list(admin_data)
            get_task_ids(admin_data, date, cam)
    else:
        admin_data = login(admin_data, filename)
        _header(admin_data)
        get_members_list(admin_data)
        get_task_ids(admin_data, date, cam)

    with open('processed_id.json', 'w', encoding='utf-8') as F:
        json.dump(processed_id, F, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    # date = datetime.now()
    main()

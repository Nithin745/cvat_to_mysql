from get_json import _header
import requests
from task_ids import task_list

TASK_URL = 'https://cvat.drill-retail.online/api/tasks/'
def mark_as_processed(task_id):
    # with open('processed_id.json', 'r') as F:
    #     data = json.load(F)
    # task_ids = data['task_id']
    headers = _header()
    payload = {"processed": True}
    url = TASK_URL + str(task_id)
    res = requests.patch(url=url, json=payload, headers=headers)
    print(res.url)
    if res.ok:
        print(f"Marked task {task_id} as processed")
    else:
        print(f"Error while marking task as processed {res.text}")

for i in task_list:
    mark_as_processed(i)

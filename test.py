import pytz
from datetime import datetime


date_after = '2022-08-18'
date_raw = datetime.strptime(date_after, '%Y-%m-%d')
date = date_raw.utcnow()
print(date)


# var = "planogram_2022-08-07_16-54-05_camera10_62f32e1177bfec30bb8d7b6a_rotem_2429"
# if 'planogram' in var:
#     x = var.lstrip('planogram_')
#     date_time = '_'.join(x.split('_', 2)[:2])
#     print(date_time)

class Hello:
    def __init__(self) -> None:
        print("Hello World")

    def __del__(self) -> None:
        print("Good bye")

    def greet(self, name):
        print(f"Hello, {name}")


for i in range(5):
    greet = Hello()
    greet.greet('Nithin')

def get_frame_no(frame):
    """This method returns seconds for the given frame"""
    frame = frame.rstrip('.PNG')

    return int(frame.split('_')[1].lstrip('0'))

numb = get_frame_no('frame_00001.PNG')
print(numb)

sd = [{'id':1}, {'id': 2}]
print(**sd)

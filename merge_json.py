from copy import copy, deepcopy
from dataclasses import dataclass, field
from typing import Optional, List, Tuple
import shutil
import json
import os

@dataclass
class Annotation:
    id: int
    bbox: List[int]
    image_id: int
    attributes: dict
    task_id: int
    category_name: str

@dataclass
class Annotations:
    width: int
    height: int
    task_id: int
    file_name: str
    annotations: List[Annotation]
    
@dataclass
class Task:
    name: str
    video: str
    task_id: int
    project_id: int
    created_date: str
    project_name: str
    completed_date: str
    validation_date: Optional[str]
    camera_id: str


class MergeJson:

    def __init__(self, path, file, dest_path) -> None:
        with open(path, 'r') as f:
            self.data = json.load(f)
        self.file = file
        self.dest_path = dest_path

    def merge_json(self):


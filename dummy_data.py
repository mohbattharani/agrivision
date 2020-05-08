import os
import math
from random import shuffle
from datetime import datetime, timedelta
from typing import Optional, Dict, List

import cfg


def rename_move(file_paths: List,  destination_path: str, start_time: Optional[str] = '09-00-00',
                end_time: Optional[str] = '18-30-00') -> None:

    start_time = datetime.strptime(start_time, '%H-%M-%S')
    end_time = datetime.strptime(end_time, '%H-%M-%S')
    diff = end_time - start_time
    inc = math.floor(diff.seconds / len(file_paths))

    for file_path in file_paths:
        file_name = start_time.strftime('%H-%M-%S') + '.jpg'
        os.rename(file_path, os.path.join(destination_path, file_name))
        start_time += timedelta(seconds=inc)


def data_split(file_list: List, num_splits: int) -> List:
    split_length = len(file_list) // num_splits
    splits = list()
    start_index = 0
    for split_num in range(num_splits):

        if (split_num + 1) is not num_splits:
            splits.append((start_index, start_index + split_length))
            start_index += split_length

        else:
            splits.append((start_index, len(file_list) - 1))

    return splits


def filter_data(dir_path: str, cam_info: Dict, num_days: Optional[int] = 5) -> None:
    for cam_id in cam_info.keys():
        path = os.path.join(dir_path, cam_id)
        image_dir = os.path.join(os.path.join(path, 'JPEGImages'))
        images = os.listdir(image_dir)
        image_paths = [os.path.join(image_dir, image) for image in images]
        splits = data_split(file_list=images, num_splits=num_days)
        starting_date = datetime.now() + timedelta(days=1)

        for start, end in splits:
            # path check
            date = starting_date.strftime('%Y-%m-%d')
            folder_path = os.path.join(path, date)
            if not os.path.exists(folder_path):
                os.mkdir(folder_path)
            # rename and move
            image_list = image_paths[start:end]
            rename_move(file_paths=image_list, destination_path=folder_path)
            # update folder
            starting_date += timedelta(days=1)  # Add to move on to next date


def folder_division(dir_path: str, src_folder: str, cam_info: Dict):
    path = os.path.join(dir_path, src_folder)
    image_dir = os.path.join(os.path.join(path, 'JPEGImages'))
    images = os.listdir(image_dir)
    shuffle(images)
    image_paths = [os.path.join(image_dir, image) for image in images]
    camera_splits = data_split(file_list=images, num_splits=len(cam_info))
    count = 0
    for cam_id in cam_info.keys():
        start, end = camera_splits[count]
        folder_path = os.path.join(dir_path, cam_id)
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
            os.mkdir(folder_path, 'JPEGImages')
        image_list = image_paths[start:end]
        rename_move(file_paths=image_list, destination_path=os.path.join(folder_path, 'JPEGImages'))
        count +=1


if __name__ == '__main__':
    # filter_data(dir_path=cfg.directories.get('main_dir'), cam_info=cfg.cam_info)
    folder_division(dir_path=cfg.directories.get('main_dir'), src_folder='VOC_DATASET', cam_info=cfg.cam_info)
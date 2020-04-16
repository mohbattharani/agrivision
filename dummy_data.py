import os
from datetime import datetime, timedelta
from typing import Union, Optional, Dict, List

import cfg


def rename_move(file_paths: List,  destination_path: str, start_time: Optional[str] = '09-00-00',
                end_time: Optional[str] = '06-30-00') -> None:

    start_time = datetime.strptime(start_time, '%H-%M-%S')
    end_time = datetime.strptime(end_time, '%H-%M-%S')
    diff = start_time - end_time
    total_minutes = diff.seconds/60
    minutes_inc = total_minutes / len(file_paths)

    for file_path in file_paths:
        file_name = start_time.strftime('%H-%M-%S') + '.jpg'
        os.rename(file_path, os.path.join(destination_path, file_name))
        start_time += timedelta(minutes=minutes_inc)


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
        image_dir = os.path.join(os.path.join('JPEGImages', path))
        images = os.listdir(image_dir)
        splits = data_split(file_list=images, num_splits=num_days)
        starting_date = datetime.now() + timedelta(days=1)

        for start, end in splits:
            # path check
            date = starting_date.strftime('%Y-%m-%d')
            folder_path = os.path.join(path, date)
            if not os.path.exists(folder_path):
                os.mkdir(folder_path)
            # rename and move
            image_list = images[start:end]
            rename_move(file_paths=image_list, destination_path=folder_path)
            # update folder
            starting_date += timedelta(days=1)  # Add to move on to next date


if __name__ == '__main__':
    filter_data(dir_path=cfg.directories.get('main_dir'), cam_info=cfg.cam_info)

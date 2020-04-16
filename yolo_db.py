import os

from PIL import Image
from pymongo import MongoClient

import cfg
from model.yolo import YOLO

client = MongoClient(cfg.mongo_cfg.get('db_server').get('host'), int(cfg.mongo_cfg.get('db_server').get('port')))
db = client[cfg.mongo_cfg.get('db_name')]
collection = db[cfg.mongo_cfg.get('db_raw_clc')]


def db(yolo_model, save=False):
    while True:
        documents = collection.find({"Predictions": {"$exists": False}})
        for document in documents:
            id = document.get('_id')
            cam_id, folder_name, image_name = id.split('_')
            # cam_id = id[0]
            # folder_name = id[1]
            # image_name = id[2]
            image_path = os.path.join(cfg.directories.get('main_dir'), cam_id,
                                      folder_name, image_name + '.jpg')
            image = Image.open(image_path)

            if save is True:
                image, annot = yolo_model.detect_image(image, save)
                save_dir = cfg.directories.get('save_dir')
                if not os.path.exists(save_dir):
                    os.mkdir(save_dir)
                image.save(os.path.join(save_dir,cam_id,folder_name,image_name + '.jpg'))
            else:
                annot = yolo_model.detect_image(image)

            collection.update_one({'_id': id}, {'$set': {'Predictions': annot}})


if __name__ == '__main__':
    yolo_model = YOLO()
    db(yolo_model, save=False)
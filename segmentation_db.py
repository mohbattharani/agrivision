import os

from pymongo import MongoClient

import cfg
from SG_model.script import predict_

client = MongoClient(cfg.mongo_cfg.get('db_server').get('host'), int(cfg.mongo_cfg.get('db_server').get('port')))
db = client[cfg.mongo_cfg.get('db_name')]
collection = db[cfg.mongo_cfg.get('db_raw_clc')]


def db(model):
    while True:
        documents = collection.find({"SG_Predictions": {"$exists": False}})
        for document in documents:
            id = document.get('_id')
            cam_id, folder_name, image_name = id.split('_')
            image_path = os.path.join(cfg.directories.get('main_dir'), cam_id,
                                      folder_name, image_name + '.jpg')
            # image = Image.open(image_path)

            output = model(image_path)

            collection.update_one({'_id': id}, {'$set': {'SG_Predictions': output}})


if __name__ == '__main__':
    db(predict_)
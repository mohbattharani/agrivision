from pymongo import MongoClient, GEO2D
# noinspection PyUnresolvedReferences
import cfg

# Mongo initialization
client = MongoClient(cfg.mongo_cfg.get('db_server').get('host'), int(cfg.mongo_cfg.get('db_server').get('port')))


class api_call:
    def __init__(self, db, clc):
        # specify which db to use
        self.db = client[db]
        # specify which collection to use
        self.collection = self.db[clc]

    def image_count(self):
        return self.collection.count()

    def image_count_camid(self, camid):
        return self.collection.find({"cam_id": camid})

    def trash_count(self):
        documents = self.collection.find({"Predictions": {"$exists": True}})
        count = 0
        for document in documents:
            pred = document.get("Predictions")
            count += len(pred)

        return count

    def trash_count_camid(self, camid):
        documents = self.collection.find({"cam_id": camid, "Predictions": {"$exists": True}})
        count = 0
        for document in documents:
            pred = document.get("Predictions")
            count += len(pred)

        return count


if __name__ == '__main__':
    server = api_call(cfg.mongo_cfg.get('db_name'), cfg.mongo_cfg.get('db_raw_clc'))


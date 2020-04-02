from pymongo import MongoClient, GEO2D
# noinspection PyUnresolvedReferences
import cfg
from datetime import datetime, timedelta
import numpy as np
# Mongo initialization
client = MongoClient(cfg.mongo_cfg.get('db_server').get('host'), int(cfg.mongo_cfg.get('db_server').get('port')))


class api_call:
    def __init__(self, db, clc):
        # specify which db to use
        self.db = client[db]
        # specify which collection to use
        self.collection = self.db[clc]

    def image_count(self, camid=None):
        if camid is None:
            return self.collection.count()
        else:
            return self.collection.find({"cam_id": camid})

    def trash_count(self, camid=None):
        if camid is None:
            documents = self.collection.find({"Predictions": {"$exists": True}})
        else:
            documents = self.collection.find({"cam_id": camid, "Predictions": {"$exists": True}})
        count = 0
        for document in documents:
            pred = document.get("Predictions")
            count += len(pred)

        return count

    def graph_day(self, date, camid=None):
        if camid is None:
            documents = self.collection.find({"date": date, "Predictions": {"$exists": True}})
        else:
            documents = self.collection.find({"cam_id": camid, "date": date, "Predictions": {"$exists": True}})
        times = []
        count = []
        for document in documents:
            pred = document.get("Predictions")
            times.append(document.get("time"))
            count.append(len(pred))

        return times, count

    def graph_range(self, start_date, end_date, date_format='%Y-%m-%d', camid=None):
        # start and end date must be in the format YYYY-MM-DD
        days_diff = (datetime.strptime(start_date, date_format) - datetime.strptime(end_date, date_format)).days
        dates = np.array([(start_date + timedelta(inc)).strftime('%Y-%m-%d') for inc in range(0, days_diff + 1)])
        count = np.zeros_like(dates)
        if camid is None:
            documents = self.collection.find({"date": {'$in': dates}, "Predictions": {"$exists": True}})
        else:
            documents = self.collection.find({"cam_id": camid, "date": {'$in': dates}, "Predictions": {"$exists": True}})
        for document in documents:
            pred = document.get("Predictions")
            date = document.get("date")
            index = np.where(dates==date)[0]
            count[index] += len(pred)

        return dates, count


if __name__ == '__main__':
    server = api_call(cfg.mongo_cfg.get('db_name'), cfg.mongo_cfg.get('db_raw_clc'))


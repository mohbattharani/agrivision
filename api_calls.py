import operator
from datetime import datetime, timedelta
import enum

import numpy as np
from pymongo import MongoClient

# noinspection PyUnresolvedReferences
import cfg

# Mongo initialization
client = MongoClient(cfg.mongo_cfg.get('db_server').get('host'), int(cfg.mongo_cfg.get('db_server').get('port')))


class Months(enum.Enum):
    January = '01'
    February = '02'
    March = '03'
    April = '04'
    May = '05'
    June = '06'
    July = '07'
    August = '08'
    September = '09'
    October = '10'
    November = '11'
    December = '12'


class ApiCall:
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

    # get documents that contain predictions
    def prediction_documents(self, camid=None):
        if camid is None:
            documents = self.collection.find({"Predictions": {"$exists": True}})
        else:
            documents = self.collection.find({"cam_id": camid, "Predictions": {"$exists": True}})
        return documents

    def trash_count(self, camid=None):
        documents = self.prediction_documents(camid=camid)
        count = 0
        for document in documents:
            pred = document.get("Predictions")
            count += len(pred)

        return count

    def day_graph(self, date, camid=None):
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

        return dict(zip(times, count))

    def range_graph(self, start_date, end_date, date_format='%Y-%m-%d', camid=None):
        # start and end date must be in the format YYYY-MM-DD
        days_diff = abs((datetime.strptime(start_date, date_format) - datetime.strptime(end_date, date_format)).days)
        dates = [(datetime.strptime(start_date, date_format) + timedelta(inc)).strftime('%Y-%m-%d')
                 for inc in range(0, days_diff + 1)]

        count = np.zeros(len(dates))
        if camid is None:
            documents = self.collection.find({"date": {'$in': dates}, "Predictions": {"$exists": True}})
        else:
            documents = self.collection.find({"cam_id": camid, "date": {'$in': dates}, "Predictions": {"$exists": True}})
        dates = np.array(dates)
        for document in documents:
            pred = document.get("Predictions")
            date = document.get("date")
            index = np.where(dates == date)[0]
            count[index] += len(pred)

        return dict(zip(dates, count))

    # calculating time for maximum trash
    def max_trash_hours(self, camid=None):
        '''
        :return: A dictionary containing time slot along with quantity of trash.
        Time slot is taken in the hourly range. If time slot is 11, then it represents time from  11am to 12pm
        '''
        documents = self.prediction_documents(camid=camid)

        times = []
        count = []

        for document in documents:
            pred = document.get("Predictions")
            times.append(document.get("time"))
            count.append(len(pred))

        # time filtering
        u_times = list(set(times))
        count = np.array(count)
        trash_count_hours = np.zeros(23)  # 24 hours time
        for time in u_times:
            mask = np.where(times == time)[0]
            trash_count = np.sum(count[mask])
            time_slot = int(time.split('-')[0])
            trash_count_hours[time_slot] += trash_count

        time_slots = list(map(str, list(range(24))))
        trash_count_hour = dict(zip(time_slots, trash_count_hours))
        # max trash hour
        # max_hour = str(max(trash_count_hours)) + '-' + str(max(trash_count_hours)+ 1)
        return trash_count_hour

    @staticmethod
    def day_data_filtering(documents):
        # All data with predictions but it has to be filtered to get total trash in a single date
        count_data = []
        dates_data = []

        for document in documents:
            dates_data.append(document.get("date"))
            count_data.append(len(document.get("Predictions")))

        # Converting all data into single day
        dates = list(set(dates_data))
        trash_count = np.array(count_data)
        trash_count_days = {}
        for date in dates:
            mask = np.where(dates_data == date)[0]  # mask to filter out date
            trash_count = np.sum(trash_count[mask])
            trash_count_days.update({date: trash_count})
        return trash_count_days

    def max_trash_days(self, camid=None):
        documents = self.prediction_documents(camid=camid)

        total_days_month = np.zeros(31)
        for document in documents:
            date = document.get("date")
            day_index = int(date.split('-')[2])
            total_days_month[day_index] += len(documents.get("Predictions"))
        max_day = str(max(total_days_month))

        # trash_count_days = self.day_data_filtering(documents)
        # trash_count_days = {}
        # for month in Months:
            # filter_month = {k: v for (k, v) in trash_count_days if f'-{month.value}-' in k}
            # max_day = max(filter_month.items(), key=operator.itemgetter(1))[0]
            # trash_count_days.update({max_day[0]: max_day[1]})

        # from collections import OrderedDict
        # OrderedDict(sorted(a.items(), key=lambda x: x[1]))

        return max_day

    def max_trash_month(self, camid=None):
        documents = self.prediction_documents(camid=camid)

        trash_count_days = self.day_data_filtering(documents)

        trash_count_month = {}
        for month in Months:
            filter_month = {k: v for (k, v) in trash_count_days if f'-{month.value}-' in k}
            total_trash = sum(filter_month.values())
            trash_count_month.update({month.name: total_trash})

        # returns month containing max trash
        # max_month = max(trash_count_monthly.items(), key=operator.itemgetter(1))[0]
        return trash_count_month


if __name__ == '__main__':
    serve = ApiCall(cfg.mongo_cfg.get('db_name'), cfg.mongo_cfg.get('db_raw_clc'))
    # print(serve.range_graph(start_date='2020-04-26', end_date='2020-04-28'))

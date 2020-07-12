"""
This script contains api calls which can be used to extract meaningful data from the Mongo Database

This script requires the numpy and schedule library to be installed. The database server and port also need to be
defined in the configuration file.

This script can also be imported as a module and contains the ODApiCall class.
"""

import operator
from datetime import datetime, timedelta
import enum
from typing import Optional, Dict

import numpy as np
from pymongo import MongoClient

# noinspection PyUnresolvedReferences
import cfg

# Mongo initialization
client = MongoClient(cfg.mongo_cfg.get('db_server').get('host'), int(cfg.mongo_cfg.get('db_server').get('port')))


class Months(enum.Enum):
    """
    Enum Class containing month numbering
    """
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


class ODApiCall:
    """
    A class for extracting meaningful data from the MongoDB Database

    Attributes
    ----------
    db : MongoClient
        Database to be initialized and used for storing data
    collection : Dict
        Contains documents containing dota about each image captured from camera nodes.

    Methods
    -------
    image_count(camid=None)
        Calculates total number of images of all camera nodes or a specific camera node if camid is specified
    trash_count(camid=None, date=None)
        Calculates total number of trash detected according to parameters specified.
    day_graph(camid=None, date=None)
        Calculates number of trash according to time for a specified day.
    range_graph(start_date, end_date, date_format='%Y%m%d', camid=None)
        Calculates total number of trash per day between specified date range
    max_trash_hours(camid=None)
        Calculates the time for maximum trash detected during the day
    max_trash_days(camid=None)
        Calculates Day of the month which gives the maximum trash
    max_trash_month(camid=None)
        Calculates month which gives maximum trash over the year in the dataset
    """

    def __init__(self, db: str, clc: str):
        """
        Parameters
        ----------
        db : str
            Name of the Database to be initialized and used
        clc: str
            Name of the collection in the database where data will be stored
        """

        # specify which db to use
        self.db = client[db]
        # specify which collection to use
        self.collection = self.db[clc]

    def image_count(self, camid: Optional[str] = None):
        """
        Calculates total number of images of all camera nodes or a specific camera node if camid is specified

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None).

        Returns
        -------
        Image count
        """

        if camid is None:
            return self.collection.count()
        else:
            return self.collection.find({"cam_id": camid}).count()

    # get documents that contain predictions
    def prediction_documents(self, camid: Optional[str] = None, date: Optional[str] = None):
        """
        Retrieves all present data or data specific to defined parameters from the database

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None).
        date : str, optional
            Date for which the data is supposed to be retrieved (Default is None).

        Returns
        -------
        documents : Dict
        Dictionary of dictionaries containing data regarding images
        """

        if camid is None and date is None:
            documents = self.collection.find({"OD_Predictions": {"$exists": True}})

        elif camid is None and date is not None:
            documents = self.collection.find({"date": date, "OD_Predictions": {"$exists": True}})

        elif camid is not None and date is None:
            documents = self.collection.find({"cam_id": camid, "OD_Predictions": {"$exists": True}})

        else:
            documents = self.collection.find({"cam_id": camid, "date": date, "OD_Predictions": {"$exists": True}})

        return documents

    def trash_count(self, camid: Optional[str] = None, date: Optional[str] = None):
        """
        Calculates total number of trash detected according to parameters specified.

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None).
        date : str, optional
            Date for which the data is supposed to be retrieved (Default is None).

        Returns
        -------
        count : int
            Total number of trash predictions
        """

        documents = self.prediction_documents(camid=camid, date=date)
        count = 0
        for document in documents:
            pred = document.get("OD_Predictions")
            count += len(pred)

        return count

    def day_graph(self, camid: Optional[str] = None, date: Optional[str] = None):
        """
        Calculates number of trash according to time for a specified day.

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved.
        date : str, optional
            Date for which the data is supposed to be retrieved

        Returns
        -------
        Dict containing times of the day with their corresponding number of trash detected
        """

        documents = self.prediction_documents(camid=camid, date=date)

        times = []
        count = []

        for document in documents:
            pred = document.get("OD_Predictions")
            times.append(document.get("time"))
            count.append(len(pred))

        return dict(zip(times, count))

    def range_graph(self, start_date: str, end_date: str, date_format: Optional[str] = '%Y-%m-%d', camid: Optional[str] = None):
        """
        Calculates total number of trash per day between specified date range

        Parameters
        ----------
        start_date : str
            Starting Date from which data  is supposed to be retrieved
        end_date : str
            Ending Date till which data is supposed to be retrieved
        date_format : str, optional
            Date format of the starting and ending date (Default is '%Y-%m-%d')
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None)

        Returns
        -------
        Dict containing dates with their corresponding number of trash predictions
        """

        # start and end date must be in the format YYYY-MM-DD
        days_diff = abs((datetime.strptime(start_date, date_format) - datetime.strptime(end_date, date_format)).days)
        dates = [(datetime.strptime(start_date, date_format) + timedelta(inc)).strftime('%Y-%m-%d')
                 for inc in range(0, days_diff + 1)]

        count = np.zeros(len(dates))
        if camid is None:
            documents = self.collection.find({"date": {'$in': dates}, "OD_Predictions": {"$exists": True}})
        else:
            documents = self.collection.find({"cam_id": camid, "date": {'$in': dates},
                                              "OD_Predictions": {"$exists": True}})

        dates = np.array(dates)
        for document in documents:
            pred = document.get("OD_Predictions")
            date = document.get("date")
            # Find index of the date in dates array
            index = np.where(dates == date)[0]
            # Add total number of trash detected to the corresponding index
            count[index] += len(pred)

        return dict(zip(dates, count))

    def max_trash_hours(self, camid: Optional[str] = None):
        """
        Calculates the time for maximum trash detected during the day

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None)

        Returns
        -------
        max_hour : str
            Hour for which the trash detected is maximum during the day
        """

        documents = self.prediction_documents(camid=camid)

        times = []
        count = []

        for document in documents:
            pred = document.get("OD_Predictions")
            times.append(document.get("time"))
            count.append(len(pred))

        # time filtering
        u_times = list(set(times))
        count = np.array(count)
        trash_count_hours = np.zeros(23)  # 24 hours time
        for time in u_times:
            # Find indexes containing time in times
            mask = np.where(times == time)[0]
            # Sum to calculate total number of trash
            trash_count = np.sum(count[mask])
            # Get hour
            time_slot = int(time.split('-')[0])
            # Add trash count to corresponding time slot index in array
            trash_count_hours[time_slot] += trash_count

        # Uncomment the following lines to return dict containing number of trash detected along with trash hours
        # time_slots = list(map(str, list(range(24))))
        # trash_count_hour = dict(zip(time_slots, trash_count_hours))
        # max trash hour
        max_hour = str(np.argmax(trash_count_hours)) + '-' + str(np.argmax(trash_count_hours) + 1)

        return max_hour

    @staticmethod
    def day_data_filtering(documents: Dict):
        """
        Calculates total trash detected for each date in the database

        Parameters
        ----------
        documents : Dict
            Dictionary containing dictionaries of data for each image

        Returns
        -------
        trash_count_days : dict
            Dict containing dates and their corresponding number of trash predictions
        """

        # All data with predictions but it has to be filtered to get total trash in a single date
        count_data = []
        dates_data = []

        for document in documents:
            dates_data.append(document.get("date"))
            count_data.append(len(document.get("OD_Predictions")))

        # Converting all data into single day
        dates = list(set(dates_data))
        trash_count = np.array(count_data)
        trash_count_days = {}
        for date in dates:
            # mask to filter out date
            mask = np.where(dates_data == date)[0]
            trash_count = np.sum(trash_count[mask])
            trash_count_days.update({date: trash_count})
        return trash_count_days

    def max_trash_days(self, camid: Optional[str] = None):
        """
        Calculates Day of the month which gives the maximum trash

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None)

        Returns
        -------
        max_day : str
            Day which gives the maximum trash predictions in the month over the whole dataset
        """

        documents = self.prediction_documents(camid=camid)

        total_days_month = np.zeros(31)
        for document in documents:
            date = document.get("date")
            day_index = int(date.split('-')[2])
            total_days_month[day_index] += len(documents.get("OD_Predictions"))
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

    def max_trash_month(self, camid: Optional[str] = None):
        """
        Calculates month which gives maximum trash over the year in the dataset

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None)

        Returns
        -------
        max_month : str
            Name of the month which gives maximum trash
        """

        documents = self.prediction_documents(camid=camid)

        trash_count_days = self.day_data_filtering(documents)

        trash_count_month = {}

        for month in Months:
            filter_month = {k: v for (k, v) in trash_count_days if f'-{month.value}-' in k}
            total_trash = sum(filter_month.values())
            trash_count_month.update({month.name: total_trash})

        # returns month containing max trash
        max_month = max(trash_count_month.items(), key=operator.itemgetter(1))[0]

        return max_month


class SGApiCall:
    def __init__(self, db: str, clc: str):
        """
        Parameters
        ----------
        db : str
            Name of the Database to be initialized and used
        clc: str
            Name of the collection in the database where data will be stored
        """

        # specify which db to use
        self.db = client[db]
        # specify which collection to use
        self.collection = self.db[clc]

    # get documents that contain predictions
    def prediction_documents(self, camid: Optional[str] = None, date: Optional[str] = None):
        """
        Retrieves all present data or data specific to defined parameters from the database

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None).
        date : str, optional
            Date for which the data is supposed to be retrieved (Default is None).

        Returns
        -------
        documents : Dict
        Dictionary of dictionaries containing data regarding images
        """

        if camid is None and date is None:
            documents = self.collection.find({"SG_Predictions": {"$exists": True}})

        elif camid is None and date is not None:
            documents = self.collection.find({"date": date, "SG_Predictions": {"$exists": True}})

        elif camid is not None and date is None:
            documents = self.collection.find({"cam_id": camid, "SG_Predictions": {"$exists": True}})

        else:
            documents = self.collection.find({"cam_id": camid, "date": date, "SG_Predictions": {"$exists": True}})

        return documents

    def trash_area(self, camid: Optional[str] = None, date: Optional[str] = None):
        """
        Calculates total area of trash detected according to parameters specified.

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None).
        date : str, optional
            Date for which the data is supposed to be retrieved (Default is None).

        Returns
        -------
        count : int
            Total number of trash predictions
        """

        documents = self.prediction_documents(camid=camid, date=date)
        trash_area = 0
        for document in documents:
            pred = document.get("SG_Predictions")
            trash_area += pred['Trash_Area']

        return trash_area

    def day_graph(self, camid: Optional[str] = None, date: Optional[str] = None):
        """
        Calculates number of trash according to time for a specified day.

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved.
        date : str, optional
            Date for which the data is supposed to be retrieved

        Returns
        -------
        Dict containing times of the day with their corresponding number of trash detected
        """

        documents = self.prediction_documents(camid=camid, date=date)

        times = []
        trash_area = []

        for document in documents:
            pred = document.get("SG_Predictions")
            times.append(document.get("time"))
            trash_area.append(pred['Trash_Area'])

        return dict(zip(times, trash_area))

    def range_graph(self, start_date: str, end_date: str, date_format: Optional[str] = '%Y-%m-%d', camid: Optional[str] = None):
        """
        Calculates total number of trash per day between specified date range

        Parameters
        ----------
        start_date : str
            Starting Date from which data  is supposed to be retrieved
        end_date : str
            Ending Date till which data is supposed to be retrieved
        date_format : str, optional
            Date format of the starting and ending date (Default is '%Y-%m-%d')
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None)

        Returns
        -------
        Dict containing dates with their corresponding number of trash predictions
        """

        # start and end date must be in the format YYYY-MM-DD
        days_diff = abs((datetime.strptime(start_date, date_format) - datetime.strptime(end_date, date_format)).days)
        dates = [(datetime.strptime(start_date, date_format) + timedelta(inc)).strftime('%Y-%m-%d')
                 for inc in range(0, days_diff + 1)]

        trash_area = np.zeros(len(dates))
        if camid is None:
            documents = self.collection.find({"date": {'$in': dates}, "SG_Predictions": {"$exists": True}})
        else:
            documents = self.collection.find({"cam_id": camid, "date": {'$in': dates},
                                              "SG_Predictions": {"$exists": True}})

        dates = np.array(dates)
        for document in documents:
            pred = document.get("SG_Predictions")
            date = document.get("date")
            # Find index of the date in dates array
            index = np.where(dates == date)[0]
            # Add total number of trash detected to the corresponding index
            trash_area[index] += len(pred)

        return dict(zip(dates, trash_area))

    def max_trash_hours(self, camid: Optional[str] = None):
        """
        Calculates the time for maximum trash detected during the day

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None)

        Returns
        -------
        max_hour : str
            Hour for which the trash detected is maximum during the day
        """

        documents = self.prediction_documents(camid=camid)

        times = []
        trash_area = []

        for document in documents:
            pred = document.get("SG_Predictions")
            times.append(document.get("time"))
            trash_area.append(pred['Trash_Area'])

        # time filtering
        u_times = list(set(times))
        area = np.array(trash_area)
        trash_count_hours = np.zeros(23)  # 24 hours time
        for time in u_times:
            # Find indexes containing time in times
            mask = np.where(times == time)[0]
            # Sum to calculate total number of trash
            trash_count = np.sum(area[mask])
            # Get hour
            time_slot = int(time.split('-')[0])
            # Add trash count to corresponding time slot index in array
            trash_count_hours[time_slot] += trash_count

        # Uncomment the following lines to return dict containing number of trash detected along with trash hours
        # time_slots = list(map(str, list(range(24))))
        # trash_count_hour = dict(zip(time_slots, trash_count_hours))
        # max trash hour
        max_hour = str(np.argmax(trash_count_hours)) + '-' + str(np.argmax(trash_count_hours) + 1)

        return max_hour

    @staticmethod
    def day_data_filtering(documents: Dict):
        """
        Calculates total trash detected for each date in the database

        Parameters
        ----------
        documents : Dict
            Dictionary containing dictionaries of data for each image

        Returns
        -------
        trash_count_days : dict
            Dict containing dates and their corresponding number of trash predictions
        """

        # All data with predictions but it has to be filtered to get total trash in a single date
        count_data = []
        dates_data = []

        for document in documents:
            dates_data.append(document.get("date"))
            count_data.append(len(document.get("SG_Predictions")))

        # Converting all data into single day
        dates = list(set(dates_data))
        trash_count = np.array(count_data)
        trash_count_days = {}
        for date in dates:
            # mask to filter out date
            mask = np.where(dates_data == date)[0]
            trash_count = np.sum(trash_count[mask])
            trash_count_days.update({date: trash_count})
        return trash_count_days

    def max_trash_days(self, camid: Optional[str] = None):
        """
        Calculates Day of the month which gives the maximum trash

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None)

        Returns
        -------
        max_day : str
            Day which gives the maximum trash predictions in the month over the whole dataset
        """

        documents = self.prediction_documents(camid=camid)

        total_days_month = np.zeros(31)
        for document in documents:
            date = document.get("date")
            day_index = int(date.split('-')[2])
            total_days_month[day_index] += len(documents.get("SG_Predictions"))
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

    def max_trash_month(self, camid: Optional[str] = None):
        """
        Calculates month which gives maximum trash over the year in the dataset

        Parameters
        ----------
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None)

        Returns
        -------
        max_month : str
            Name of the month which gives maximum trash
        """

        documents = self.prediction_documents(camid=camid)

        trash_count_days = self.day_data_filtering(documents)

        trash_count_month = {}

        for month in Months:
            filter_month = {k: v for (k, v) in trash_count_days if f'-{month.value}-' in k}
            total_trash = sum(filter_month.values())
            trash_count_month.update({month.name: total_trash})

        # returns month containing max trash
        max_month = max(trash_count_month.items(), key=operator.itemgetter(1))[0]

        return max_month


if __name__ == '__main__':
    serve = ODApiCall(cfg.mongo_cfg.get('db_name'), cfg.mongo_cfg.get('db_raw_clc'))
    # print(serve.range_graph(start_date='2020-04-26', end_date='2020-04-28'))

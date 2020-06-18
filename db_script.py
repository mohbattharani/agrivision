"""
This script retrieves daily data of all camera nodes from the synced camfeed folder.

This script requires schedule and pymongo to be installed. The database server and port also need to be defined in the
configuration file.

This script can also be used as a module and contains the DbUp class.
"""

import os
import time
from datetime import datetime, timedelta
from typing import Optional
import warnings

import schedule
from pymongo import MongoClient, GEO2D
from pymongo.errors import DuplicateKeyError

# noinspection PyUnresolvedReferences
import cfg

# Mongo initialization
client = MongoClient(cfg.mongo_cfg.get('db_server').get('host'), int(cfg.mongo_cfg.get('db_server').get('port')))


class DbUp:
    """
    A class used for initializing database and adding data of images received daily.

    Attributes
    ----------
    db : MongoClient
        Database to be initialized and used for storing data
    collection : Dict
        Contains documents containing dota about each image captured from camera nodes.
    date_format : str, optional
        Date format of the starting and ending date (Default is '%Y-%m-%d')

    Methods
    -------
    update_24
        Calls add_to_database function every 24 hours
    get_all_data(start_date, num_days=5)
        Adds data from the specified starting date till the number of days to the database
    """

    def __init__(self, db: str, clc: str, starting_date: Optional[str] = None, date_format: Optional[str] = '%Y-%m-%d'):
        """
        Parameters
        ----------
        db : str
            Name of the Database to be initialized and used
        clc: str
            Name of the collection in the database where data will be stored
        starting_date : str, optional
            starting date from which the data is supposed to be added to database
        """

        # specify which db to use
        self.db = client[db]
        # specify which collection to use
        self.collection = self.db[clc]
        # creating index for GEO spatial data
        self.collection.create_index([('location', GEO2D)])

        self.starting_date = starting_date
        self.date_format = date_format


    def images_add(self, date: str):
        """
        Adds image data to the database collection for all cameras

        Parameters
        ----------
        date: str
            Date for which the data is retrieved and added

        Warnings
        --------
        UserWarning
            If a duplicate entry is being made to the database.
        UserWarning
            If the data for a specified date does not exist.
        """

        for cam in cfg.cam_info.keys():
            # get image list
            try:
                images = os.listdir(os.path.join(cfg.directories.get('main_dir'), cam, date))
                # prepare images to post
                for image in images:
                    # get image time through its name
                    img_time = image.split('.')[0]
                    post = {
                        '_id':      cam + '_' + date + '_' + img_time,
                        'cam_id':   cam,
                        'filename': image,
                        'date':     date,
                        'time':     img_time,
                        'location': [cfg.cam_info.get(cam).get('longitude'), cfg.cam_info.get(cam).get('latitude')],
                        'description': cfg.cam_info.get(cam).get('description')
                    }

                    try:
                        self.collection.insert_one(post)
                    except DuplicateKeyError:
                        warnings.warn('Duplicate file')
                        continue

            except FileNotFoundError:
                warnings.warn('Folder for {} date does not exist'.format(date))
                # TODO
                # Dump this folder date to some array to process it later
        current_date = datetime.now().strftime(self.date_format)
        print('database updated on {}'.format(current_date))

    def add_to_database(self):
        """
        Adds yesterday data to the database by using the image_add function
        """

        current_date = datetime.now()
        # get last date by subtracting 1 day time from current date
        yesterday_date = current_date - timedelta(days=1)

        print('Adding Images')

        # check if starting date is mentioned
        if self.starting_date is not None:
            days_diff = abs((yesterday_date - datetime.strptime(self.starting_date, self.date_format)).days)
            for date_inc in range(days_diff+1):
                date = (datetime.strptime(self.starting_date, self.date_format) + timedelta(days=date_inc)).strftime(
                                                                                                    self.date_format)
                self.images_add(date)
        else:
            y_date_format = yesterday_date.strftime(self.date_format)
            self.images_add(y_date_format)

    def update_24(self):
        """
        Calls add_to_database function every 24 hours
        """

        schedule.every().day.at('00:01').do(self.add_to_database)
        # schedule.every(25).seconds.do(self.add_to_database)
        print('Database is UP')
        # schedule.run_all()
        while True:
            schedule.run_pending()
            time.sleep(1)

    def get_all_data(self, start_date: str, num_days: Optional[int] = 5):
        """
        Adds data from the specified starting date till the number of days to the database

        Parameters
        ----------
        start_date : str
            Starting date from which database is supposed to be added
        num_days : int
            Number of days from starting date which are supposed to be added
        """

        # get last date by subtracting 1 day time from current date
        start_date = datetime.strptime(start_date, self.date_format)

        for day in range(num_days + 1):
            date = start_date + timedelta(days=day)
            data_date = date.strftime(self.date_format)
            print(f'Adding Images for date {data_date}')
            self.images_add(data_date)


if __name__ == '__main__':
    server = DbUp(cfg.mongo_cfg.get('db_name'), cfg.mongo_cfg.get('db_raw_clc'), starting_date='2020-05-10')
    server.update_24()
    # server.get_all_data('2020-05-10', num_days=25)
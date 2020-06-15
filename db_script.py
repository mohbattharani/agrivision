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

    Methods
    -------
    update_24
        Calls add_to_database function every 24 hours
    get_all_data(start_date, num_days=5)
        Adds data from the specified starting date till the number of days to the database
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
        # creating index for GEO spatial data
        self.collection.create_index([('location', GEO2D)])


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
        print('database updated on {}'.format(date))

    def add_to_database(self):
        """
        Adds yesterday data to the database by using the image_add function
        """

        current_date = datetime.now()
        # get last date by subtracting 1 day time from current date
        yesterday_date = current_date - timedelta(days=1)
        y_date_format = yesterday_date.strftime('%Y-%m-%d')
        print('Adding Images')

        self.images_add(y_date_format)

    def update_24(self):
        """
        Calls add_to_database function every 24 hours
        """

        # schedule.every().day.at('00:01').do(self.add_to_database)
        schedule.every(25).seconds.do(self.add_to_database)
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
        start_date = datetime.strptime(start_date, '%Y-%m-%d')

        for day in range(num_days + 1):
            date = start_date + timedelta(days=day)
            date_format = date.strftime('%Y-%m-%d')
            print(f'Adding Images for date {date_format}')
            self.images_add(date_format)


if __name__ == '__main__':
    server = DbUp(cfg.mongo_cfg.get('db_name'), cfg.mongo_cfg.get('db_raw_clc'))
    # server.update_24()
    server.get_all_data('2020-05-10', num_days=25)
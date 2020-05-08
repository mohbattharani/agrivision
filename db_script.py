import os
import time
from datetime import datetime, timedelta

import schedule
from pymongo import MongoClient, GEO2D
from pymongo.errors import DuplicateKeyError

# noinspection PyUnresolvedReferences
import cfg

# Mongo initialization
client = MongoClient(cfg.mongo_cfg.get('db_server').get('host'), int(cfg.mongo_cfg.get('db_server').get('port')))


class DbUp:
    def __init__(self, db, clc):
        # specify which db to use
        self.db = client[db]
        # specify which collection to use
        self.collection = self.db[clc]
        # creating index for GEO spatial data
        self.collection.create_index([('location', GEO2D)])

# Add to database
    def images_add(self, date):
        # posts = []
        for cam in cfg.cam_info.keys():
            # get image list
            try:
                images = os.listdir(os.path.join(cfg.directories.get('main_dir'), cam, date))
                # prepare images to post
                for image in images:
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
                # posts.append(post)
                    try:
                        self.collection.insert_one(post)
                    except DuplicateKeyError:
                        continue
            except FileNotFoundError:
                print('Folder for {} date does not exist'.format(date))
                # TODO
                # Dump this folder date to some array to process it later
        print('database updated on {}'.format(date))

    # add to database after every 24 hours
    def add_to_database(self):
        current_date = datetime.now()
        # get last date by subtracting 1 day time from current date
        yesterday_date = current_date - timedelta(days=1)
        y_date_format = yesterday_date.strftime('%Y-%m-%d')
        print('Adding Images')
        self.images_add(y_date_format)

    # update after 24 hours
    def update_24(self):
        # schedule.every().day.at('00:01').do(self.add_to_database)
        schedule.every(25).seconds.do(self.add_to_database)
        print('Database is UP')
        # schedule.run_all()
        while True:
            schedule.run_pending()
            time.sleep(1)

    # add all available data to database
    def get_all_data(self, start_date, num_days=5):
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
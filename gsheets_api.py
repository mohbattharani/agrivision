from pathlib import Path
from typing import Optional, List, Union
from datetime import datetime, timedelta
import json
import time

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import numpy as np
import schedule

import cfg


class GoogleSheetApi:
    __DEFAULT_SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    def __init__(self, scope: Optional[List] = None,
                 credential_path: Optional[Union[str, Path]] = r'drive-credentials.json',
                 sheet_name: Optional[str] = r'Trash_DB', starting_date: Optional[str] = None,
                 date_format: Optional[str] = r'%Y-%m-%d', reset: Optional[bool] = False):
        # Initialize scope and access spreadsheet
        gscope = scope if scope else GoogleSheetApi.__DEFAULT_SCOPE
        creds = ServiceAccountCredentials.from_json_keyfile_name(credential_path, gscope)
        client = gspread.authorize(creds)
        self.spreadsheet = client.open(sheet_name)
        self.starting_date = starting_date
        self.date_format = date_format
        self.sheet_reset = reset

    def worksheet_exists(self, sheet_id, all_stats_flag=False):
        # Get the list of worksheets available
        worksheet_list = [worksheet.title for worksheet in self.spreadsheet.worksheets()]
        # Create worksheet if it does not exist
        if sheet_id not in worksheet_list:
            self.spreadsheet.add_worksheet(title=sheet_id, rows=1, cols=2)
            sheet = self.spreadsheet.worksheet(title=sheet_id)
            sheet.resize(1)
            if all_stats_flag:
                sheet.append_row(['Camera ID', 'Total Trash Quantity', 'Lat, Long'])
            else:
                sheet.append_row(['Date', 'Trash Quantity'])

    def initialize_worksheets(self):
        self.worksheet_exists('Total')

        for camid in cfg.cam_info.keys():
            self.worksheet_exists(camid)

        self.worksheet_exists('All Stats', all_stats_flag=True)

    @property
    def day_diff_check(self) -> bool:
        latest_date = datetime.now() - timedelta(days=1)
        if self.starting_date is not None:
            days_diff = abs((latest_date - datetime.strptime(self.starting_date, self.date_format)).days)
            return True if days_diff > 0 else False
        else:
            return False

    def update_sheet(self, sheet_title, camid=None):
        sheet = self.spreadsheet.worksheet(title=sheet_title)
        if self.sheet_reset:
            sheet.resize(1)
        else:
            sheet.resize(sheet.row_count)
        # Get latest date which should be the previous day as data is stored after each day is complete
        latest_date = datetime.now() - timedelta(days=1)

        if self.day_diff_check:  # Checks for starting date mention and compares with current date
            start_date = self.starting_date
            end_date = latest_date.strftime(self.date_format)

            if camid is not None:
                req = {"start_date": start_date, "end_date": end_date, 'camid': camid}
            else:
                req = {"start_date": start_date, "end_date": end_date}

            req = json.dumps(req)
            x = requests.post(cfg.api_urls.get('range_data'), data=req)
            x = json.loads(x.text)
            dates, trash_count = zip(*x.items())
            trash_count = list(map(int, trash_count))
            n = len(dates)
            dates, trash = np.asarray(dates).reshape(n, 1), np.asarray(trash_count).reshape(n, 1)
            final_array = np.concatenate((dates, trash), axis=1)
            sheet.append_rows(final_array.tolist())

        else:
            date = latest_date
            if camid is not None:
                req = {"date": date, "camid": camid}
            else:
                req = {"date": date}

            req = json.dumps(req)
            x = requests.post(cfg.api_urls.get('total_trash'), data=req)
            trash_count = json.loads(x.text)
            sheet.append_row([date, trash_count])

    def update_all_stats(self):
        all_stats_sheet = self.spreadsheet.worksheet(title='All Stats')
        all_stats_sheet.resize(1)
        update_list = list()
        # 1. Get total trash quantity
        sheet = self.spreadsheet.worksheet(title='Total')
        daily_trash = sheet.col_values(2)
        daily_trash.pop(0)
        trash_quant = sum(list(map(int, daily_trash)))
        update_list.append(['Total', trash_quant])
        # 2. Get total trash from of camera id by summing up the trash quantity columns of each camera
        for camid in cfg.cam_info.keys():
            sheet = self.spreadsheet.worksheet(title=camid)
            daily_trash = sheet.col_values(2)
            daily_trash.pop(0)
            trash_quant = sum(list(map(int, daily_trash)))
            lat_long = ','.join((str(cfg.cam_info.get(camid).get('latitude')),
                                 str(cfg.cam_info.get(camid).get('longitude'))))
            update_list.append([camid, trash_quant, lat_long])

        all_stats_sheet.append_rows(update_list)

    def update_spreadsheet(self):
        # Initialize Worksheets
        self.initialize_worksheets()

        # Update sheets
        # 1. Update Total Sheet
        self.update_sheet(sheet_title='Total')
        # 2. Update Camera Sheets
        for camid in cfg.cam_info.keys():
            self.update_sheet(sheet_title=camid, camid=camid)

        # Set reset flag to false if already set to True
        if self.sheet_reset:
            self.sheet_reset = False
        # 3. Update All Stats Sheet
        self.update_all_stats()

        # Set starting date to None after adding data to sheet
        self.starting_date = None

    def update_24(self):
        schedule.every().day.at('09:00').do(self.update_spreadsheet)
        print('Google Sheet API is Running')
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    gsheet_api = GoogleSheetApi(starting_date='2020-05-10', reset=True)
    gsheet_api.update_24()

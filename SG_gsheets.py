"""
This script is used for communication with google sheets and updating them with meaningful data from MongoDB database.
"""

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
    """
    Class for Google Sheets communications to store meaningful information from MongoDB database

    Attributes
    ----------
    spreadsheet
        Google Sheet for storing data on google drive
    starting_date : str
        starting date from which the data is supposed to be retrieved
    date_format : str
        Date format of the starting and ending date
    sheet_reset : bool
        Flag for clearing out all sheets and recomputing results

    Methods
    -------
    update_24(time='09:00')
        Calls the update_worksheet method at a specified time everyday
    """

    _DEFAULT_SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    def __init__(self, scope: Optional[List] = None,
                 credential_path: Optional[Union[str, Path]] = r'drive-credentials.json',
                 sheet_name: Optional[str] = r'SG_DB', starting_date: Optional[str] = None,
                 date_format: Optional[str] = r'%Y-%m-%d', reset: Optional[bool] = False):
        """
        Parameters
        ----------
        scope : list, optional
            Scope of access granted from google drive (Default is None)
        credential_path : str, optional
            Path to the credential file for google drive access (Default is 'drive-credentials.json')
        sheet_name : str, optional
            Name of the sheet to be accessed (Default is 'SG_DB')
        starting_date : str, optional
            starting date from which the data is supposed to be retrieved
        date_format : str, optional
            Date format of the starting and ending date (Default is '%Y-%m-%d')
        reset : bool, optional
            Flag for clearing out all sheets and recomputing results
        """

        # Initialize scope and access spreadsheet
        gscope = scope if scope else GoogleSheetApi._DEFAULT_SCOPE
        creds = ServiceAccountCredentials.from_json_keyfile_name(credential_path, gscope)
        client = gspread.authorize(creds)

        self.spreadsheet = client.open(sheet_name)
        self.starting_date = starting_date
        self.date_format = date_format
        self.sheet_reset = reset

    def worksheet_exists(self, sheet_id, all_stats_flag: Optional[bool] = False) -> None:
        """
        Checks if the specified sheet exists. If it does not then it creates it.


        Parameters
        ----------
        sheet_id : str
            Name of the sheet to be checked
        all_stats_flag : bool, optional
            Flag for all stats sheet
        """

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
        """
        Initializes the worksheets by using the worksheet_exists function
        """

        self.worksheet_exists('Total')

        for camid in cfg.cam_info.keys():
            self.worksheet_exists(camid)

        self.worksheet_exists('All Stats', all_stats_flag=True)

    @property
    def day_diff_check(self) -> bool:
        """
        Checks if the entered dates are correct

        Returns
        -------
        Bool value (True or False)
        """

        latest_date = datetime.now() - timedelta(days=1)
        if self.starting_date is not None:
            days_diff = abs((latest_date - datetime.strptime(self.starting_date, self.date_format)).days)
            return True if days_diff > 0 else False
        else:
            return False

    def update_sheet(self, sheet_title, camid: Optional[str ] = None):
        """
        Updates specified sheet with dates and their corresponding number of predictions from MongoDB.

        Parameters
        ----------
        sheet_title : str
            Name of the sheet to be updated
        camid : str, optional
            Camera ID of camera node for which the data is supposed to be retrieved (Default is None)
        """

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
                req = {"start_date": start_date, "end_date": end_date, 'camid': camid, 'model': 'SG'}
            else:
                req = {"start_date": start_date, "end_date": end_date, 'model': 'SG'}

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
            date = latest_date.strftime(self.date_format)
            if camid is not None:
                req = {"date": date, "camid": camid, 'model': 'SG'}
            else:
                req = {"date": date, 'model': 'SG'}

            req = json.dumps(req)
            x = requests.post(cfg.api_urls.get('total_trash'), data=req)
            trash_count = json.loads(x.text)
            sheet.append_row([date, trash_count])

    def update_all_stats(self):
        """
        Updates all stats sheet
        """

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
            update_list.append([cfg.cam_info.get(camid).get('description'), trash_quant, lat_long])

        all_stats_sheet.append_rows(update_list)

    def update_worksheet(self):
        """
        Updates all worksheets
        """

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

    def update_24(self, update_time: Optional[str] = '09:02'):
        """
        Calls the update_worksheet method at a specified time everyday

        Parameters
        ----------
        update_time : str, optional
            Time for updating worksheets in 24 hour format (Default set to '09:00')
        """

        schedule.every().day.at(update_time).do(self.update_worksheet)
        print('Google Sheet API is Running')
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    gsheet_api = GoogleSheetApi(starting_date='2020-05-10', reset=True)
    gsheet_api.update_24()

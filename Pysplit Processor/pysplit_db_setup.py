"""
Created on Tues Dec 5 08:00:00 2018

@author: BB

This creates the SQLite DB that will be used to track trajectories that have
been created, and ensure the met files needed to create them are stored locally.
It is intended to be run once, prior to running the processor.

By default, it adds all possible 1-H increments from 1/1/2017
through 1/1/2018. See docs for pd.date_range() in pandas to create
other timesteps.

It is currently configured to parse only hrrra files from the database, but a new
function could be substituted for get_hrrra_file_list() if desired.
"""

import os
import sqlite3
from datetime import datetime
from pathlib import Path
import pandas as pd
from ftplib import FTP

homedir = Path(os.getcwd())
local_hrrr_dir = homedir / 'met/hrrr'
remote_hrrr_dir = r'/archives/hrrr/'

site_dates = pd.date_range(start='1/1/2017', end='1/1/2018', freq= '1H', tz='MST').tz_convert('UTC').tolist()
# matrix of all dates in UTC (for model input) that require trajectories

os.chdir(homedir)

db_con = sqlite3.connect('pysplit_runs.sqlite') # connect to local DB
db_cur = db_con.cursor()

proceed = input('Do you wish to reset/create the database? This will overwrite any previous work. (y/n) ')

if proceed is 'y':

    db_cur.executescript('''
      DROP TABLE IF EXISTS files;
      DROP TABLE IF EXISTS trajectories;

      CREATE TABLE files (
          id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
          fn TEXT UNIQUE,
          traj_date TEXT,
          local     BOOLEAN,
          remote     BOOLEAN,
          needed_for_month  BOOLEAN
      );

      CREATE TABLE trajectories (
          id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
          traj_date TEXT UNIQUE,
          fn TEXT UNIQUE,
          processed BOOLEAN,
          attempted BOOLEAN
      );

    ''')

    # 'files' is the table for all met files available remotely
    #       'remote' should be all 1
    #       'local' are those available locally
    #       'needed_for_month' is the subset needed to process the month in question

    for date in site_dates:
        date = datetime.strftime(date, '%Y-%m-%d %H:%M:%S') # convert to string for DB
        db_cur.execute('''INSERT OR IGNORE INTO trajectories (traj_date, processed, attempted)
                    VALUES ( ?, ?, ? )''', ( date, 0, 0) ) # place date and label as un-processed
                    # file name will be added when it's processed and saved

    ftp_con = FTP('arlftp.arlhq.noaa.gov') # connect to NOAA's FTP server
    ftp_con.login() # login as anonymous and move to correct dir
    ftp_con.cwd(remote_hrrr_dir)

    def get_hrrra_file_list(conn):
        '''
        This function takes one FTP connection (to the ARL Server) and returns
        a list of all the hrrr met files in that connection's cwd
        '''
        remote_out = []
        conn.dir(remote_out.append)
        remote_files = []
        for line in remote_out:
            if 'hrrra' in line:
                remote_files.append(line.split(' ')[-1])
        return remote_files

    def parse_files_for_dates(met_file_list):
        file_dates = []
        for file in met_file_list:
            met_date = ''.join(file.split('.')[1:3])
            file_dates.append(datetime.strptime(met_date,'%Y%m%d%Hz'))

        return file_dates

    remote_met_files = get_hrrra_file_list(ftp_con) # list of all available met files on the server
    met_file_dates = parse_files_for_dates(remote_met_files) # list of all available dates from met files on the server

    for filename, date in zip(remote_met_files, met_file_dates):
        date = datetime.strftime(date, '%Y-%m-%d %H:%M:%S') # convert to string for DB
        db_cur.execute('''INSERT OR IGNORE INTO files (fn, traj_date, local, remote)
                    VALUES ( ?, ?, ?, ? )''', ( filename, date, 0, 1))
                    # insert all remote files and their dates
                    # mark as available remote, not-available local

    db_con.commit() # save everything and finish

    # Every 1H trajectory run up until this hour is now in pysplit_runs.sqlite,
    # and can now be recalled and checked off as the processor goes. A list of
    # available met files is also ready for retrieval based on the data needed.

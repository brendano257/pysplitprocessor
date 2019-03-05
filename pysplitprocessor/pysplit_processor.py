import os
import sqlite3
import datetime as dt
from datetime import datetime
import pandas as pd
from ftplib import FTP
from pathlib import Path
import pysplit
import pytz
"""
This should be run after pysplit_db_setup.py, which configures the database this
will work from.
"""

trajectory_runtime = -12

homedir = Path(os.getcwd())
local_hrrr_dir = homedir / 'met/hrrr'
remote_hrrr_dir = r'/archives/hrrr/'
hysplit_dir  = r'C:/hysplit4/working'
out_dir = homedir / 'trajectories'

coords = (40.07,-105.22)  # coordinates to be run

altitude = 5 # HYSPLIT defaults to AGL, this is a guess...

def get_ftp_file(fn_local, fn_remote):
    '''
    Use internal FTP connection and save fn_remote from its CWD to the
    local file fn_local, which is usually the same.
    '''
    ftp_con = FTP('arlftp.arlhq.noaa.gov') # connect to NOAA's FTP server
    ftp_con.login() # login as anonymous and move to correct dir
    ftp_con.cwd(remote_hrrr_dir)

    with open(fn_local,'wb') as localfile:
        ftp_con.retrbinary('RETR ' + fn_remote, localfile.write, 1024)
        print(f'{fn_remote} retreived and saved as {fn_local}')
    ftp_con.quit()

os.chdir(homedir)

db_con = sqlite3.connect('pysplit_runs.sqlite')  # connect to local DB
db_cur = db_con.cursor()

db_cur.execute('SELECT * FROM trajectories WHERE processed = 0 AND attempted = 0')  # find all trajectories that haven't been run

def parse_db_trajectories_select(fetched_all):
    """Pulls all information returned by SQL queries for un-processed trajectories"""
    ids = [line[0] for line in fetched_all]
    dates = [pytz.utc.localize(datetime.strptime(line[1], '%Y-%m-%d %H:%M:%S')) for line in fetched_all]
    processed_status = [line[3] for line in fetched_all]

    return (ids, dates, processed_status)

[traj_ids,traj_dates,processed_status] = parse_db_trajectories_select(db_cur.fetchall())  # get list of all un-processed trajectory dates

traj_date_groups = pd.period_range(start = traj_dates[0], end = traj_dates[-1], freq = '1W')
# make a list of all week-long groups in the unprocessed data

for period in traj_date_groups:
    print(f'Processing trajectories for the period {period}.')

    met_dates_to_process = []

    for date in traj_dates:
        start = pytz.utc.localize(period.start_time - dt.timedelta(hours=8))
        end = pytz.utc.localize(period.end_time + dt.timedelta(hours=8))
        # add more than 6 hours on each end to buffer met files

        if start <= date < end:
            met_dates_to_process.append(date)

    if len(met_dates_to_process) == 0:
        print(f'No met files on server found for processing the period {period} so it was skipped.')
        continue

    met_start_date = met_dates_to_process[0] - dt.timedelta(hours = abs(trajectory_runtime*2))  # buffer met files by 2x the runtime for safety
    met_end_date = met_dates_to_process[-1] + dt.timedelta(hours = abs(trajectory_runtime*2))

    db_cur.execute('SELECT * FROM files WHERE remote = 1')

    def parse_db_files_select(fetched_all):
        """Process the SQL query return for files."""
        ids = [line[0] for line in fetched_all]
        filenames = [line[1] for line in fetched_all]
        dates = [pytz.utc.localize(datetime.strptime(line[2], '%Y-%m-%d %H:%M:%S')) for line in fetched_all]
        local_status = [line[3] for line in fetched_all]
        remote_status = [line[4] for line in fetched_all]
        needed_for_month = [line[5] for line in fetched_all]

        return (ids, filenames, dates, local_status, remote_status, needed_for_month)

    [met_ids, filenames, met_dates, local_status, remote_status, needed_for_month] = (
        parse_db_files_select(db_cur.fetchall())) # get all file info from db

    os.chdir(local_hrrr_dir)

    fns_to_download = []

    for filename, date in zip(filenames, met_dates):  # create download list of met files needed for this period
        if met_start_date <= date <= met_end_date:
            fns_to_download.append(filename)
            # append all file names
            # print(fns_to_download)

    # local directory cleanup
    with os.scandir(local_hrrr_dir) as local_files:
        for file in local_files:
            if file.name in fns_to_download:
                fns_to_download.remove(file.name)
                # do nothing locally if file needed; remove from download list
            else:
                # otherwise, delete local file; it's no longer needed
                os.remove(file.name)

    for filename in fns_to_download:
        get_ftp_file(filename, filename) # get and save as same name
        # status prints are embedded in get_ftp_file()

        db_cur.execute('SELECT id FROM files WHERE fn = ?',(filename,))
        ind = db_cur.fetchone()[0]  # find filename in local DB, update below if found

        if ind is None:
            print(f'File {filename} not processed into database correctly.')
            continue
        else:
            db_cur.execute('''UPDATE OR IGNORE files SET (local) = (?)
                    WHERE id = ?''', (1, ind))
            db_con.commit()
            # update db to reflect the file is now available locally

    print(f'All files for period {period} retrieved.')

    # all met files are now in local_hrrr_dir
    # met_dates_to_process is now a list of trajectory-hours to process (in UTC!!!)

    def get_hrrra_met_files(traj_date, runtime, met_dir):
        """
        Create a list of met files for a trajectory at traj_date with a running time
        of runtime.
        """

        if runtime < 0: # if a back-trajectory, set end of period to given date
            traj_end = traj_date + dt.timedelta(hours = 7)
            traj_start = traj_date + dt.timedelta(hours = runtime-7)
        else:
            traj_start = traj_date - dt.timedelta(hours = 7)
            traj_end = traj_date + dt.timedelta(hours = runtime+7)

        met_files = []

        os.chdir(met_dir)

        with os.scandir() as files: # check specifically for hrrr met files
            for file in files:
                if len(file.name.split('.')) == 4:
                    datestring = (file.name.split('.')[1] + file.name.split('.')[2]).replace('z','')
                    ts = pd.to_datetime(datestring, format='%Y%m%d%H').tz_localize('UTC')
                    if (ts >= traj_start) & (ts <= traj_end):
                        met_files.append(file.name)

                else:
                    continue

        return (met_files, traj_start, traj_end)

    for date in met_dates_to_process:  # generate trajectories for each date in met_dates_to_process
        [met_files, start, end] = get_hrrra_met_files(date, trajectory_runtime, local_hrrr_dir)

        print(f'Trajectory for {date} being processed.')
        print(f'Met files for {start} to {end}: {met_files}')

        [trajname, err] = pysplit.generate_singletraj(f'fc_csu_12hr_',hysplit_dir,out_dir,
                                               local_hrrr_dir,met_files,date.year,
                                               date.month,date.day, date.hour,altitude,
                                               coords,trajectory_runtime)
        # generate_singletraj now returns the generated filename AND if the run failed err = 1 == fail

        if err == 0:
            # If no error, print and then update DB to reflect it being done
            print(f'Trajectory for {date} generated.')

            db_cur.execute('SELECT id FROM trajectories WHERE traj_date = ?',
                           (datetime.strftime(date,'%Y-%m-%d %H:%M:%S'),))
            ind = db_cur.fetchone()[0]

            if ind is None:
                print(f'Trajectory for date {date} not found in database.')
                continue
            else:
                db_cur.execute('''UPDATE OR IGNORE trajectories SET (processed, fn, attempted) = (?, ?, ?)
                        WHERE id = ?''', (1, trajname, 0, ind))
                db_con.commit() #add traj name when processed, and commit
        else:
            # Otherwise, print then set as attempted but not processed in the DB. It can be retried later
            print(f'Trajectory for {date} NOT generated due to file moving/Hysplit Error.')

            db_cur.execute('SELECT id FROM trajectories WHERE traj_date = ?',
                           (datetime.strftime(date,'%Y-%m-%d %H:%M:%S'),))
            ind = db_cur.fetchone()[0]

            if ind is None:
                print(f'Trajectory for date {date} not processed into database correctly.')
                continue
            else:
                db_cur.execute('''UPDATE OR IGNORE trajectories SET (processed, fn, attempted) = (?, ?, ?)
                        WHERE id = ?''', (0, None, 1, ind))
                db_con.commit() # set as unprocessed and w/o traj name, mark as attempted, commit

    print(f'All trajectories (except where error announced) for period {period} have been processed.')

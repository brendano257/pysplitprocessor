# pysplitprocessor

This is a processor for creating large numbers of Hysplit trajectories for specific dates. 
Running Hysplit locally can require up to 12GB of met data PER DAY that one wishes to run. This requires massive amounts of storage, or a different approach.
I chose to process a week at a time, automating the entire processs so that under 100GB of storage is needed to progressively process as many weeks as needed.
It does this by first creating a SQLite database containing all the dates that need to be run, then processing them one week at a time, pausing in between weeks to download the new data needed from the NOAA ARL server.

The included example will run 12-hour back-trajectories for all hours from 2017-1-1 to 2018-1-1, see NOTES below to make changes to this and other parameters.

This relies on Pysplit version 0.3.4.3 (brendano257/pysplit) in order to generate trajectories one at a time. 
Mellissa Warner (mscross) deserves all the credit for this, I only modified one function to create generate_singletraj() for my purposes. See mscross/pysplit for better instructions and the full project.

To install:

1) Create a folder for the project wherever you like.

2) Follow Pysplit instructions to create a virtual conda environment and install the dependencies for PySplit (using Anaconda Prompt, running the two lines below)

  conda config --add channels conda-forge
  conda create --name name_of_environment python=3.6 numpy matplotlib pandas basemap six fiona shape geopandas

3) Clone brendano257/pysplit from GitHub (https://github.com/brendano257/pysplit), and put the PySplit folder in your project folder

4) Activate the conda virtual environment
  activate name_of_environment
  
5) Change directory to your project folder, then the PySplit folder, and run

  cd “folder/of/project/pysplit-master”
  python setup.py install
  
It should confirm you’ve installed Pysplit 0.3.4.3

6) Clone brendano257/pysplitprocessor, and place the pysplitprocessor folder in your project folder, then run
pysplit_db_setup.py and pysplit_processor.py to begin running (default) trajectories, and placing the created files in /trajectories of that folder. (https://github.com/brendano257/pysplitprocessor)

  cd “folder/of/project/pysplitprocessor
  python pysplit_db_setup.py
  python pysplit_processor.py
  
 
The processor can be cancelled at any time, and what has been done will be persisted in pysplit_runs.db, a SQLite database in that folder. (DB Browser for SQLite (https://sqlitebrowser.org/) is a good way to manually view it) 

You do NOT need to re-run pysplit_db_setup.py in order to resume running, as it will overwrite any data you have made previously. If the processor was exited by the user, just restart it with “python pysplit_processor.py” from it’s directory and it will begin either running trajectories or downloading the files it needs to continue running.

## Notes:
DEFAULT: The above will only run for the default coordinates, but this can be edited in pysplit_processor.py, the variable “coords” is a (lat, long) tuple.

DEFAULT: The above will automatically run trajectories (back 12 hours) for all hours in from 1/1/2017 to 1/1/2018. Change the site_dates variable in pysplit_db_setup.py to change the run times/periods, or provide your own list of Python/Pandas datetimes 

DEFAULT: The above will run 12-hour back-trajectories by default. Change the runtime variable in pysplit_processor.py. Negative numbers indicate back-trajectories

Files aside from the HRRR 3KM data can be used, but will require the user to write/edit several functions to do this. One of them is get_hrrra_met_files() that parses the NOAA data specifically for HRRRA files

"""
- In Terminal, run "python -m venv .venv"
- Press Ctrl + Shift + P, select 'Python: Select Interpreter',
    then 'Enter interpreter path' manually in ".venv\Scripts\python.exe"
- Update installed pip "python -m pip install --upgrade pip"
- Install requirements "python -m pip install -r requirements.txt
- To Run using Terminal:
    - type "python -B main.py <scoring engine id> <specific date> <column name>"    --> For specific project and date
    - type "python -B main.py <start date> <end date> <column name>"                --> For all project and date range
    - type "python -B main.py <column name>"                                        --> For all project of today's date
"""

import sys
import json
from datetime import datetime
from shared.common import *
from shared.engines import *
from shared.constants import *
from shared.repositories import *


def get_config():
    with open('local_settings.json') as file:
        local_settings = json.load(file)

    db_data = local_settings.get('Values')
    return db_data


class LogDiff:
    def __init__(self) -> None:

        self.df_list=[]
        self.diff_list=[]
        self.name_list=[]
        self.query_list=[]
        self.differences=[]
        self.create_date_list=[]
        self.scoring_engine_ids=[]

        self.az_df = None
        self.log_diff_df = None
        self.az_sql_name = None

        self.db_data = get_config()
        self.freq = self.db_data.get('Frequency')
        self.env = self.db_data.get('Environment')
        self.trigger_time_stamp = datetime.now()
        self.trigger_time_string = self.trigger_time_stamp.strftime("%Y-%m-%d %H:%M:%S")

        self.repo = Repository()
        scoring_engine = self.repo.get_scoring_engine_ids(self.freq)
        if not scoring_engine.empty and len(scoring_engine) > 0:
            for idx, row in scoring_engine.iterrows():
                self.scoring_engine_ids.append(row['ScoringEngineId'])

    def get_log_diff_from_se_id_date(self, scoring_engine_id, date_diff, column_name):
        result_df = self.parse_compare(scoring_engine_id, self.freq, date_diff, self.env, column_name, counts=True)
        print(result_df)
        with pd.ExcelWriter(diff_report) as writer:  
            result_df.to_excel(writer, index=False)

    def get_log_diff_from_date_range(self, start_date, end_date, column_name):
        date_range_list=[]
        date_ranges = pd.date_range(start=start_date, end=end_date)

        for date_range in date_ranges:
            date_range_list.append(str(date_range.date()))
        
        for date_diff in date_range_list:
            for scoring_engine_id in self.scoring_engine_ids:
                date_range_df = self.parse_compare(scoring_engine_id, self.freq, date_diff, self.env, column_name, counts=True)
            print(date_range_df)
            with pd.ExcelWriter(date_range_report) as writer:  
                date_range_df.to_excel(writer, index=False)

    def get_log_diff_from_today(self, column_name):
        date_diff = self.trigger_time_stamp.strftime("%Y-%m-%d")
        for scoring_engine_id in self.scoring_engine_ids:
            date_today_df = self.parse_compare(scoring_engine_id, self.freq, date_diff, self.env, column_name, counts=True)
        print(date_today_df)
        with pd.ExcelWriter(date_today_report) as writer:  
            date_today_df.to_excel(writer, index=False)
    
    def parse_compare(self, scoring_engine_id, freq, date_diff, env, column_name, counts=None):
        # Parse df, get differences, and return counts based on the same criteria
        sf_log_list=[]
        log_diff_summary = self.repo.get_log_diff_queries(scoring_engine_id, freq, env)
        if not log_diff_summary.empty and len(log_diff_summary) > 0:
            for idx, row in log_diff_summary.iterrows():
                query = (row['SQL'].replace('<<PREFIX>>', row['Prefix']).replace('<<ENV>>', row['Environment']).replace('<<TABLENAME>>', row['TableName']))
                self.query_list.append([query, row['SQLName'], row['DBKey']])

                if 'Azure' in row['SQLName']:
                    az_sql_name = row['TableName'].replace('Orch', '').replace('SE', '').replace('ResponseScore', '').strip()
                    az_df = self.repo.get_az_df(date_diff, query, row['DBKey'])
                if 'SF' in row['SQLName']:
                    sf_query = self.repo.get_sf_df(query, date_diff)
                    sf_query.rename(columns={'input_correlation_id':column_name}, inplace=True)
                    sf_log_list.append([sf_query, row['SQLName'].replace('Log Count', '').strip()])

        for sf_df, sf_sql_name in  sf_log_list:
            if counts is not None and counts == True:
                # Count differences from azure df to snowflakes df
                az_not_in_sf = get_sf_azure_diff(az_df,sf_df,column_name,counts=True)
                self.name_list.append(f'{az_sql_name} - {sf_sql_name}')
                self.diff_list.append(az_not_in_sf)
                self.create_date_list.append(date_diff)
                self.log_diff_df = pd.DataFrame({'Name': self.name_list, 'Difference': self.diff_list, 'Create Date': self.create_date_list})
            else:
                # Get column name's data differences from azure df to snowflakes df
                az_not_in_sf = get_sf_azure_diff(az_df,sf_df,column_name)
                self.log_diff_df = az_not_in_sf[column_name]
            
        return self.log_diff_df
    
    def clear_cache(self):
        print('Clearing cache...\n{}'.format(self.repo.ld_cache))
        try:
            self.repo.ld_cache.clear()
            message = "Cache successfully cleared"
        except Exception as e:
            message = f"Error clearing Cache: {str(e)}"
        print(message)


def check_if_date(date_text):
    try:
        if date_text != datetime.strptime(date_text, "%Y-%m-%d").strftime('%Y-%m-%d'):
            raise ValueError
        return True
    except ValueError:
        return False

def check_difference(*argv):
    log_diff = LogDiff()
    if len(argv) > 3:
        if check_if_date(argv[1]) is False and check_if_date(argv[2]) is True:
            log_diff.get_log_diff_from_se_id_date(argv[1], argv[2], argv[3])
    
        elif check_if_date(argv[1]) is True and check_if_date(argv[2]) is True:
            log_diff.get_log_diff_from_date_range(argv[1], argv[2], argv[3])
    
    elif len(argv) < 3 and 'clearcache' in sys.argv:
        log_diff.clear_cache()

    elif len(argv) < 3:
        log_diff.get_log_diff_from_today(argv[1])


if __name__ == "__main__":
    check_difference(*sys.argv)

"""def check_difference(argv1=None, argv2=None):
    if (argv1 is not None and check_if_date(argv1) is False) and (argv2 is not None and check_if_date(argv2) is True):
        log_diff.get_log_diff_from_se_id_date(argv1, argv2)
    
    elif (argv1 is not None and check_if_date(argv1) is True) and (argv2 is not None and check_if_date(argv2) is True):
        log_diff.get_log_diff_from_date_range(argv1, argv2)
    
    elif argv1 is None and argv2 is None:
        log_diff.get_log_diff_from_today()


if __name__ == "__main__":
    log_diff = LogDiff()
    if len(sys.argv) > 2:
        check_difference(str(sys.argv[1]), str(sys.argv[2]))
    elif len(sys.argv) == 2 and 'clearcache' in sys.argv:
        log_diff.clear_cache()
    elif len(sys.argv) == 1:
        check_difference()"""
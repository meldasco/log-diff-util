"""
- In Terminal, run "python -m venv .venv"
- Press Ctrl + Shift + P, select 'Python: Select Interpreter',
    then 'Enter interpreter path' manually in ".venv\Scripts\python.exe"
- Update installed pip "python -m pip install --upgrade pip"
- Install requirements "python -m pip install -r requirements.txt"
- To Run using Terminal:
    - type "python -B main.py <scoring engine id> <specific date> <column name> diffdata"   --> For specific project, date and difference data
    - type "python -B main.py <scoring engine id> <specific date> <column name>"            --> For specific project and date
    - type "python -B main.py <start date> <end date> <column name>"                        --> For all project and date range
    - type "python -B main.py <column name>"                                                --> For all project of today's date

    *Sample Run Syntax: python -B main.py 10007 2023-10-09 correlationid diffdata
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

        self.query_list=[]
        self.az_log_list=[]
        self.sf_log_list=[]

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

    def get_log_diff_data_from_se_id_date(self, scoring_engine_id, date_diff, column_name):
        result_df = self.parse_compare(scoring_engine_id, self.freq, date_diff, self.env, column_name)
        # print(result_df)
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
        az_log_list=[]
        sf_log_list=[]
        log_diff_list=[]
        log_diff_summary = self.repo.get_log_diff_queries(scoring_engine_id, freq, env)
        if not log_diff_summary.empty and len(log_diff_summary) > 0:
            for idx, row in log_diff_summary.iterrows():
                query = (row['SQL'].replace('<<PREFIX>>', row['Prefix']).replace('<<ENV>>', row['Environment']).replace('<<TABLENAME>>', row['TableName']))
                self.query_list.append([query, row['SQLName'], row['DBKey']])

                if 'Azure' in row['SQLName']:
                    az_sql_name = row['SQLName'].replace('Log Count', '').strip()
                    az_log_df = self.repo.get_az_df(date_diff, query, row['DBKey'])
                    az_log_list.append([az_log_df, az_sql_name])
                if 'SF' in row['SQLName']:
                    sf_sql_name = row['SQLName'].replace('Log Count', '').strip()
                    sf_log_df = self.repo.get_sf_df(query, date_diff)
                    sf_log_df.rename(columns={'input_correlation_id':'correlationid'}, inplace=True)
                    sf_log_df.rename(columns={'input_correlationid':'correlationid'}, inplace=True)
                    sf_log_list.append([sf_log_df, sf_sql_name])

        for [az_df, az_name], [sf_df, sf_name] in list(zip(az_log_list, sf_log_list)):
            az_not_in_sf = get_sf_azure_diff(az_df,sf_df,column_name,counts=counts)
            self.name_list.append(f'{az_name} - {sf_name}')
            self.diff_list.append(az_not_in_sf)
            self.create_date_list.append(date_diff)
            if counts is not None and counts is True:
                self.log_diff_df = pd.DataFrame({'Name': self.name_list, 'Difference': self.diff_list, 'Create Date': self.create_date_list})
            else:
                print(az_not_in_sf)
                for i in az_not_in_sf[column_name]:
                    log_diff_list.append(i)
                log_diff_list = list(set(log_diff_list))
                self.log_diff_df = pd.DataFrame({'Difference': log_diff_list})
        return self.log_diff_df

    def clear_cache(self):
        print('Clearing cache... {}'.format(self.repo.ld_cache))
        try:
            self.repo.ld_cache.clear()
            message = "Cache successfully cleared {}".format(self.repo.ld_cache)
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
        if check_if_date(argv[1]) is False and check_if_date(argv[2]) is True and 'diffdata' in sys.argv:
            log_diff.get_log_diff_data_from_se_id_date(argv[1], argv[2], argv[3])
        
        elif check_if_date(argv[1]) is False and check_if_date(argv[2]) is True:
            log_diff.get_log_diff_from_se_id_date(argv[1], argv[2], argv[3])
    
        elif check_if_date(argv[1]) is True and check_if_date(argv[2]) is True:
            log_diff.get_log_diff_from_date_range(argv[1], argv[2], argv[3])
    
    if len(argv) < 3:
        if 'clearcache' not in sys.argv:
            log_diff.get_log_diff_from_today(argv[1])
        log_diff.clear_cache()
        

if __name__ == "__main__":
    check_difference(*sys.argv)

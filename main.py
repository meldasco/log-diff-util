import os
from datetime import datetime

from dao.dao_base import *
from shared.constants import *
from shared.helper_functions import *
from repositories.repository import *


df_list=[]
query_list=[]
differences=[]
diff_list=[]
name_list=[]
create_date_list=[]
scoring_engine_ids=[]

az_df = None
log_diff_df = None
az_sql_name = None

env = 'DEV'
freq = 'Daily'
trigger_time_stamp = datetime.now()
trigger_time_string = trigger_time_stamp.strftime("%Y-%m-%d %H:%M:%S")

repo = Repository()
scoring_engine = repo.get_scoring_engine_ids(freq)
if not scoring_engine.empty and len(scoring_engine) > 0:
    for idx, row in scoring_engine.iterrows():
        scoring_engine_ids.append(row['ScoringEngineId'])

def get_project_date_entry_log_diff():
    date_diff = '2023-07-18'
    scoring_engine_id = '100014'
    print(get_log_diff(scoring_engine_id, freq, date_diff, env, counts=True))


def get_date_range_log_diff():
    date_range_list=[]
    start_date = "2023-07-12"
    end_date = "2023-07-20"
    unresolved_sf_tables = [10008]
    date_ranges = pd.date_range(start=start_date, end=end_date)

    for date_range in date_ranges:
        date_range_list.append(str(date_range.date()))
    
    for date_diff in date_range_list:
        for scoring_engine_id in scoring_engine_ids:
            if scoring_engine_id not in unresolved_sf_tables:
                date_range_df = get_log_diff(scoring_engine_id, freq, date_diff, env, counts=True)
        print(date_range_df)
        with pd.ExcelWriter(date_range_report) as writer:  
            date_range_df.to_excel(writer, index=False)


def get_today_log_diff():
    date_diff = trigger_time_stamp.strftime("%Y-%m-%d")
    unresolved_sf_tables = [10008]
    for scoring_engine_id in scoring_engine_ids:
        if scoring_engine_id not in unresolved_sf_tables:
            date_range_df = get_log_diff(scoring_engine_id, freq, date_diff, env, counts=True)
    print(date_range_df)
    with pd.ExcelWriter(today_report) as writer:  
        date_range_df.to_excel(writer, index=False)


def get_log_diff(scoring_engine_id,freq,date_diff,env,counts=None):
    # Parse df, get differences, and return counts based on the same criteria
    sf_log_list=[]
    log_diff_summary = repo.get_log_diff_queries(scoring_engine_id, freq, env)
    if not log_diff_summary.empty and len(log_diff_summary) > 0:
        for idx, row in log_diff_summary.iterrows():
            query = (row['SQL'].replace('<<PREFIX>>', row['Prefix']).replace('<<ENV>>', row['Environment']).replace('<<TABLENAME>>', row['TableName']))
            query_list.append([query, row['SQLName'], row['DBKey']])

            if 'Azure' in row['SQLName']:
                az_sql_name = row['TableName'].replace('Orch', '').replace('SE', '').replace('ResponseScore', '')
                az_df = repo.get_az_df(scoring_engine_id, date_diff)
            if 'SF' in row['SQLName']:
                sf_query = repo.get_sf_df(query, date_diff)
                sf_query.rename(columns={'input_correlation_id':'correlationid'}, inplace=True)
                sf_log_list.append([sf_query,row['SQLName'].replace('Log Count', '')])

    for sf_df, sf_sql_name in  sf_log_list:
        if counts is not None and counts == True:
             # Count differences from azure df to snowflakes df
            az_not_in_sf = get_sf_azure_diff(az_df,sf_df,'correlationid',counts=True)
        else:
             # Get column name's data differences from azure df to snowflakes df
            az_not_in_sf = get_sf_azure_diff(az_df,sf_df,'correlationid')
        
        name_list.append(f'{az_sql_name} - {sf_sql_name}')
        diff_list.append(az_not_in_sf)
        create_date_list.append(date_diff)

    log_diff_df = pd.DataFrame({'Name': name_list, 'Difference': diff_list, 'Create Date': create_date_list})
    return log_diff_df


if __name__ == '__main__':
    # get_project_date_entry_log_diff()
    get_date_range_log_diff()
    # get_today_log_diff()
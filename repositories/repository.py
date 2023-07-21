import pandas as pd

from cacheout import Cache
from datetime import datetime
from dao.dao_base import *



mr_cache = Cache()


class Repository(DAOBase):
    def __init__(self) -> None:
        super().__init__()

    def get_log_diff_queries(self, scoringEngineId:int, freq:str, environment):
        env = 'PRD' if environment == 'PROD' else environment
        sql = """
        SELECT JobMonitoringConfigurationDetailLogDiffId
        ,SQLName
        ,SQL
        ,Environment
        ,TableName
        ,Prefix
        ,DBKey
        ,diff.CreateDateTime
        ,diff.ModifiedDateTime
        ,diff.ActiveStartDateTime
        ,diff.ActiveEndDateTime
        ,diff.JobMonitoringConfigurationId
        FROM dbo.ScoringEngineJobMonitoringConfigurationDetailLogDiff diff
        INNER JOIN dbo.ScoringEngineJobMonitoringConfiguration conf
        ON conf.JobMonitoringConfigurationId = diff.JobMonitoringConfigurationId
        WHERE conf.ActiveStartDateTime <= getDate() and
        conf.ActiveEndDateTime > getDate() and
        diff.ActiveStartDateTime <= getDate() and
        diff.ActiveEndDateTime > getDate() and
        conf.ScoringEngineID = {0} and
        conf.Frequency = '{1}' and
        diff.Environment = '{2}'
        """.format(scoringEngineId, freq, env)
        result = super().read_sql_to_df(sql, 'SAMetadata')
        return result

    # def get_az_df(self, date, query, dbkey):
    #     return super().read_sql_to_df(query.format(date), dbkey)

    def get_sf_df(self, query, date):
        return super().read_sf_sql_to_df(query.format(date))

    def get_sf_fe_df(self, date):
        sql = """
        select * from "SA_UK_REGIONAL_PO_IHP_V1_PROD"."PRD"."POIHP_FE_OUTPUT"
        where DATE(CREATE_DATE) = '{0}'
        """.format(date)
        result = super().read_sf_sql_to_df(sql)
        return result
    
    def get_sf_ms_df(self, date):
        sql = """
        select * from "SA_UK_REGIONAL_PO_IHP_V1_PROD"."PRD"."POIHP_MS_OUTPUT"
        where DATE(CREATE_DATE) = '{0}'
        """.format(date)
        result = super().read_sf_sql_to_df(sql)
        return result

    def get_az_df(self, scoringEngineId:int, date):
        results_all = []
        sql = """
        SELECT SyntheticTestHealthCheckQuery
        FROM ScoringEngine
        WHERE ScoringEngineId = {0}
        """.format(scoringEngineId)
        result = super().read_sql_to_df(sql, 'SAMetadata')
        if not result.empty and len(result) > 0:
            for idx, row in result.iterrows():
                
                project_name = row['SyntheticTestHealthCheckQuery'].split('FROM ')[1].split('ResponseService')[0].replace('[','')

                az_sql = """
                select * from {0}ResponseScore where {0}InputScoreRequestid in (
                select {0}InputScoreRequestid from {0}ResponseScore 
                where CONVERT(Date, CreateDateTime) = '{1}' and Status = 'Completed')
                """.format(project_name,date)
                results = super().read_sql_to_df(az_sql, 'SAOp')
        return results # results_all #
    
    def get_az_df_prod(self, scoringEngineId:int, date):
        results_all = []
        sql = """
        SELECT SyntheticTestHealthCheckQuery
        FROM ScoringEngine
        WHERE ScoringEngineId = {0}
        """.format(scoringEngineId)
        result = super().read_sql_to_df(sql, 'SAMetadata')
        if not result.empty and len(result) > 0:
            for idx, row in result.iterrows():
                
                project_name = row['SyntheticTestHealthCheckQuery'].split('FROM ')[1].split('ResponseService')[0].replace('[','')

                az_sql = """
                select * from {0}ResponseScore where {0}InputScoreRequestid in (
                select {0}InputScoreRequestid from {0}ResponseScore 
                where CONVERT(Date, CreateDateTime) = '{1}' and Status = 'Completed')
                """.format(project_name,date)
                results = super().read_sql_to_df_prod(az_sql, 'SAOp')
        return results

    def get_scoring_engine_ids(self, freq:str):
        sql = """
        SELECT
            [ScoringEngineId]
            ,[Frequency]
        FROM [dbo].[ScoringEngineJobMonitoringConfiguration]
        WHERE Frequency = '{0}'
        """.format(freq)
        result = super().read_sql_to_df(sql, 'SAMetadata')
        return result

    def get_alert_email_to(self, scoringEngineId:int, functionName:str, environment:str):
        sql = """
        SELECT EmailTo  
        FROM [ScoringEngineEmailConfiguration] seec  
        WHERE seec.[ActiveStartDateTime] <=getDate() and  
        seec.[ActiveEndDateTime]>getDate() and  
        seec.[ScoringEngineID] = {0} and  
        seec.[Environment] = '{1}' and  
        seec.[AzureFunction] = '{2}'
        """.format(scoringEngineId, environment, functionName)
        result = super().read_sql_to_df(sql, 'SAMetadata')
        return result

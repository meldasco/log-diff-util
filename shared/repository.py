from cacheout import Cache
from shared.sql_engine import *


mr_cache = Cache()

class Repository(SQLReader):
    def __init__(self) -> None:
        super().__init__()

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

    def get_sf_df(self, query, date):
        return super().read_sf_sql_to_df(query.format(date))

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




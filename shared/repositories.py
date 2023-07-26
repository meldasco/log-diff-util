from cacheout import Cache
from shared.engines import *


# ld_cache = Cache()

class Repository(SQLReader):
    def __init__(self) -> None:
        super().__init__()
        self.ld_cache = Cache()

    def get_scoring_engine_ids(self, freq:str):
        key = f'get_scoring_engine_ids_{freq}'
        if key in self.ld_cache:
            result = self.ld_cache.get(key)
        else:
            sql = """
            SELECT
                [ScoringEngineId]
                ,[Frequency]
            FROM [dbo].[ScoringEngineJobMonitoringConfiguration]
            WHERE Frequency = '{0}'
            """.format(freq)
            result = super().read_sql_to_df(sql, 'SAMetadata')
            self.ld_cache.set(key, result)
        return result

    def get_log_diff_queries(self, scoringEngineId:int, freq:str, environment):
        key = f'get_log_diff_queries_{scoringEngineId}_{freq}_{environment}'
        if key in self.ld_cache:
            result = self.ld_cache.get(key)
        else:
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
            self.ld_cache.set(key, result)
        return result

    def get_az_df(self, date, query, dbkey):
        key = f'get_az_df_{date}_{query}_{dbkey}'
        if key in self.ld_cache:
            result = self.ld_cache.get(key)
        else:
            result = super().read_sql_to_df(query.format(date), dbkey)
            print('Parsed data from AZ: {} rows x {} columns'.format(len(result), len(result.columns)))
            self.ld_cache.set(key, result)
        return result

    def get_sf_df(self, query, date):
        key = f'get_sf_df_{query}_{date}'
        if key in self.ld_cache:
            result = self.ld_cache.get(key)
        else:
            result = super().read_sf_sql_to_df(query.format(date))
            print('Parsed data from SF: {} rows x {} columns'.format(len(result), len(result.columns)))
            self.ld_cache.set(key, result)
        return result




import os
import urllib

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from cacheout import Cache, LRUCache
from snowflake.sqlalchemy import *
from snowflake.connector import *

from .constants import *
from settings.local_settings import local_settings_dev, local_settings_prd



"""cache only 20 recently used items, removing any other least recently used items.
20 seems to be a decent size for this cache, we mostly will not outgrow this limit of environment variables"""
env_cache = LRUCache(maxsize=20)
cache = Cache()


def create_errors_collection(code, errors, schemaValidationErrors):
    if isinstance(errors, str):
        schemaValidationErrors.append(errors)
        return schemaValidationErrors
   
    for key, value in errors.items():
        mainVar = key
        for item in value:
            if isinstance(item, str):
                scheValError = {
                    "Code": "SchemaValidationError",
                    "Description": item + " : " + key,
                }
                schemaValidationErrors.append(scheValError)
            else:
                for key, value in item.items():
                    if len(str(key)) == 1:
                        for item in value:
                            for key, value in item.items():
                                childVar = key
                                desc = value[0]
                                description = f"{mainVar}.{childVar} {desc}"
                                scheValError = {
                                    "Code": code,
                                    "Description": description,
                                }
                                schemaValidationErrors.append(scheValError)
                    else:
                        childVar = key
                        desc = value[0]
                        description = f"{mainVar}.{childVar} {desc}"
                        scheValError = {"Code": code, "Description": description}
                        schemaValidationErrors.append(scheValError)

    return schemaValidationErrors


def create_azsql_engine(AZSQLDatabase=None):

    if AZSQLDatabase:
        db = AZSQLDatabase
    else:
        # db = get_env_var("AZSQLDatabase")
        db = local_settings_dev['AZSQLDatabase']

    AZSQLDriver=local_settings_dev['AZSQLDriver']
    AZSQLServer=local_settings_dev['AZSQLServer']
    AZSQLUID=local_settings_dev['AZSQLUID']
    AZSQLPWD=local_settings_dev['AZSQLPWD']
    AZSQLConnectionTimeout=local_settings_dev['AZSQLConnectionTimeout']

    params = urllib.parse.quote_plus(
        "Driver=%s;" % AZSQLDriver
        + "Server=tcp:%s,1433;" % AZSQLServer
        + "Database=%s;" % db
        + "Uid=%s;" % AZSQLUID
        + "Pwd={%s};" % AZSQLPWD
        + "Encrypt=yes;"
        + "TrustServerCertificate=no;"
        + "Connection Timeout=%s" % AZSQLConnectionTimeout
        + ";"
    )

    conn_str = "mssql+pyodbc:///?odbc_connect=" + params
    poolSize=local_settings_dev['AZSQLDatabasePoolSize'] 
    PoolMaxOverflow=local_settings_dev['AZSQLDatabasePoolMaxOverflow']
    engine = create_engine(conn_str, pool_size=int(poolSize), max_overflow=int(PoolMaxOverflow),pool_pre_ping=True)
    return engine

def create_azsql_engine_prd(AZSQLDatabase=None):

    if AZSQLDatabase:
        db = AZSQLDatabase
    else:
        # db = get_env_var("AZSQLDatabase")
        db = local_settings_prd['AZSQLDatabase']

    AZSQLDriver=local_settings_prd['AZSQLDriver']
    AZSQLServer=local_settings_prd['AZSQLServer']
    AZSQLUID=local_settings_prd['AZSQLUID']
    AZSQLPWD=local_settings_prd['AZSQLPWD']
    AZSQLConnectionTimeout=local_settings_prd['AZSQLConnectionTimeout']

    params = urllib.parse.quote_plus(
        "Driver=%s;" % AZSQLDriver
        + "Server=tcp:%s,1433;" % AZSQLServer
        + "Database=%s;" % db
        + "Uid=%s;" % AZSQLUID
        + "Pwd={%s};" % AZSQLPWD
        + "Encrypt=yes;"
        + "TrustServerCertificate=no;"
        + "Connection Timeout=%s" % AZSQLConnectionTimeout
        + ";"
    )

    conn_str = "mssql+pyodbc:///?odbc_connect=" + params
    poolSize=local_settings_prd['AZSQLDatabasePoolSize'] 
    PoolMaxOverflow=local_settings_prd['AZSQLDatabasePoolMaxOverflow']
    engine = create_engine(conn_str, pool_size=int(poolSize), max_overflow=int(PoolMaxOverflow),pool_pre_ping=True)
    return engine


def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


def get_env_var(name):
    if name in env_cache:
        return env_cache.get(name)
    value = os.getenv(name)
    env_cache.set(name, value)
    return value


def create_sf_engine():
    return create_engine(URL(
    user=local_settings_dev["SnowflakeUser"],
    password=local_settings_dev["SnowflakePassword"],
    account=local_settings_dev["SnowflakeAccount"],
    warehouse=local_settings_dev["SnowflakeWH"],
    role=local_settings_dev["SnowflakeRole"],
    database=local_settings_dev["SnowflakeDB"],
    schema=local_settings_dev["SnowflakeSchema"]
    ))


def sf_ctx():
    ctx = connect(
        user=local_settings_dev["SnowflakeUser"],
        password=local_settings_dev["SnowflakePassword"],
        account=local_settings_dev["SnowflakeAccount"],
        warehouse=local_settings_dev["SnowflakeWH"],
        role=local_settings_dev["SnowflakeRole"],
        database=local_settings_dev["SnowflakeDB"],
        schema=local_settings_dev["SnowflakeSchema"]
    )
    return ctx

def write_diff(df1, df2):
    difference = str(df1).split(output_folder)[1].replace('.csv', '') + \
        '__vs__' + str(df2).split(output_folder)[1].replace('.csv', '')
    diff =  output_folder + difference + '_diff.csv'

    with open(df1, 'r') as t1, open(df2, 'r') as t2:
        fileone = t1.readlines()
        filetwo = t2.readlines()

    with open(diff, 'w') as outFile:
        for line in filetwo:
            if line not in fileone:
                outFile.write(line)

def get_df_difference(df1,df2,column_name):
    df1.columns = map(str.lower, df1.columns)
    df2.columns = map(str.lower, df2.columns)
    column_name = column_name.casefold()
    common = df1.merge(df2, on=[column_name])
    result = df1[~df1[column_name].isin(common[column_name])]
    return result


def get_sf_azure_diff(df1,df2,column_name,counts=None):
    diff = get_df_difference(df1,df2,column_name)
    if counts is not None and counts is True:
        return len(diff)
    else:
        return diff[column_name.casefold()]
    
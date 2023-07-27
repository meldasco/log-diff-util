import os
import json
import urllib

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from cacheout import Cache, LRUCache
from snowflake.sqlalchemy import *
from snowflake.connector import *

from .constants import *


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


def create_azsql_engine(db_data, AZSQLDatabase=None):

    if AZSQLDatabase:
        db = AZSQLDatabase
    else:
        db = db_data.get('AZSQLDatabase')

    AZSQLDriver = db_data.get('AZSQLDriver')
    AZSQLServer = db_data.get('AZSQLServer')
    AZSQLUid = db_data.get('AZSQLUID')
    AZSQLPwd = db_data.get('AZSQLPWD')
    AZSQLConnectionTimeout = db_data.get('AZSQLConnectionTimeout')

    params = urllib.parse.quote_plus(
        "Driver=%s;" % AZSQLDriver
        + "Server=tcp:%s,1433;" % AZSQLServer
        + "Database=%s;" % db
        + "Uid=%s;" % AZSQLUid
        + "Pwd={%s};" % AZSQLPwd
        + "Encrypt=yes;"
        + "TrustServerCertificate=no;"
        + "Connection Timeout=%s" % AZSQLConnectionTimeout
        + ";"
    )

    conn_str = "mssql+pyodbc:///?odbc_connect=" + params
    poolSize = db_data.get('AZSQLDatabasePoolSize') 
    PoolMaxOverflow = db_data.get('AZSQLDatabasePoolMaxOverflow')
    engine = create_engine(conn_str, pool_size=int(poolSize), max_overflow=int(PoolMaxOverflow),pool_pre_ping=True)
    return engine


def create_sf_engine(db_data):
    return create_engine(URL(
    user = db_data.get('SnowflakeUser'),
    password = db_data.get('SnowflakePassword'),
    account = db_data.get('SnowflakeAccount'),
    warehouse = db_data.get('SnowflakeWH'),
    role = db_data.get('SnowflakeRole'),
    database = db_data.get('SnowflakeDB'),
    schema = db_data.get('SnowflakeSchema')
    ))


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
        return diff


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
    
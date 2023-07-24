import logging
import threading
import traceback

import pandas as pd

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy import DateTime, null, orm

from shared.helper_functions import *


def get_config():
    with open('local_settings.json') as file:
        config = json.load(file)

    db_data = config.get('database')
    freq = config.get('Frequency')
    return db_data, freq

db_data, freq = get_config()
engine_1 = create_azsql_engine(db_data,"SAMetadataOp")
def get_connection_metadata_op():
    try: 
        global engine_1 
        if((engine_1==None) or (engine_1.engine.engine._connection_cls._is_disconnect)):  
            engine_1 = create_azsql_engine(db_data,"SAMetadataOp")
        return engine_1
    except:
        traceback.print_exc()


engine_saimplementation = create_azsql_engine(db_data,"SAImplementation")
def get_connection_saimplementation ():
    try: 
        global engine_saimplementation
        if((engine_saimplementation==None) or (engine_saimplementation.engine.engine._connection_cls._is_disconnect)):  
            engine_saimplementation = create_azsql_engine(db_data,"SAImplementation")
        return engine_saimplementation
    
    except Exception as e:
        traceback.print_exc()


engine_sametadata = create_azsql_engine(db_data,"SAMetadata")
def get_connection_sametadata():
    try: 
        global engine_sametadata
        if((engine_sametadata==None) or (engine_sametadata.engine.engine._connection_cls._is_disconnect)):  
            engine_sametadata = create_azsql_engine(db_data,"SAMetadata")
        return engine_sametadata
    
    except Exception as e:
        traceback.print_exc()


engine_saop = create_azsql_engine(db_data,"SAOp")
def get_connection_saop():
    try: 
        global engine_saop 
        if((engine_saop==None) or (engine_saop.engine.engine._connection_cls._is_disconnect)):  
            engine_saop = create_azsql_engine(db_data,"SAOp")
        return engine_saop
    except:
        traceback.print_exc()


class DAOBase:
    def __init__(self):
        pass

    def generate_id(self, seqName: str):
        result = 0
        try:
            conn = get_connection_metadata_op()
            sql = "SELECT NEXT VALUE FOR {0} as batchid".format(seqName)
            result = pd.read_sql(sql, conn)
        except Exception as e:
            pass
        return int(result["batchid"][0])

    def read_sql_to_df(self, query, db_key):
        output_table = None
        if db_key is not None:
            logging.info(f"Database connection to {db_key}")
            if(db_key=="SAImplementation"):
                conn = get_connection_saimplementation()
            if(db_key=="SAMetadata"):
                conn = get_connection_sametadata()
            if(db_key=="SAOp"):
                conn = get_connection_saop()
            output_table = pd.read_sql(query, conn)
        else:
            try:
                conn = get_connection_metadata_op()
                output_table = pd.read_sql(query, conn)
            except Exception as e:
                logging.info("Try the connection again" + str(e))
                engine_1 = create_azsql_engine(db_data,"SAMetadataOp")
                output_table = pd.read_sql(query, engine_1)

        return output_table
    
    def read_sf_sql_to_df(self, query):
        return pd.read_sql(query, create_sf_engine(db_data))

import logging
import threading
import traceback

import pandas as pd

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy import DateTime, null, orm

from shared.helper_functions import *


engine_1 = create_azsql_engine("SAMetadataOp")


def get_session():
    try:   
        global engine_1
        if((engine_1==None) or (engine_1.engine.engine._connection_cls._is_disconnect)):      
            engine_1 = create_azsql_engine("SAMetadataOp") 
        return orm.Session(bind=engine_1)
    except:
        traceback.print_exc()
    

def get_connection_metadata_op():
    try: 
        global engine_1 
        if((engine_1==None) or (engine_1.engine.engine._connection_cls._is_disconnect)):  
            engine_1 = create_azsql_engine("SAMetadataOp")
        return engine_1
    except:
        traceback.print_exc()


engine_saimplementation = create_azsql_engine("SAImplementation")
def get_connection_saimplementation ():
    try: 
        global engine_saimplementation
        if((engine_saimplementation==None) or (engine_saimplementation.engine.engine._connection_cls._is_disconnect)):  
            engine_saimplementation = create_azsql_engine("SAImplementation")
        return engine_saimplementation
    
    except Exception as e:
        traceback.print_exc()


engine_sametadata = create_azsql_engine("SAMetadata")
def get_connection_sametadata():
    try: 
        global engine_sametadata
        if((engine_sametadata==None) or (engine_sametadata.engine.engine._connection_cls._is_disconnect)):  
            engine_sametadata = create_azsql_engine("SAMetadata")
        return engine_sametadata
    
    except Exception as e:
        traceback.print_exc()


engine_saop = create_azsql_engine("SAOp")
def get_connection_saop():
    try: 
        global engine_saop 
        if((engine_saop==None) or (engine_saop.engine.engine._connection_cls._is_disconnect)):  
            engine_saop = create_azsql_engine("SAOp")
        return engine_saop
    except:
        traceback.print_exc()

engine_saop_prd = create_azsql_engine_prd("SAOp")
def get_connection_saop_prd():
    try: 
        global engine_saop_prd 
        if((engine_saop_prd==None) or (engine_saop_prd.engine.engine._connection_cls._is_disconnect)):  
            engine_saop_prd = create_azsql_engine_prd("SAOp")
        return engine_saop_prd
    except:
        traceback.print_exc()


engine_sf_sync_logs = create_sf_engine()
def get_sf_engine():
    try:  
        global engine_sf_sync_logs    
        if((engine_sf_sync_logs==None) or (engine_sf_sync_logs.engine.engine._connection_cls._is_disconnect)):
            engine_sf_sync_logs = create_sf_engine()
        return engine_sf_sync_logs
       
    except Exception as e:
        traceback.print_exc()


def commit_with_retry(objToSave, session, mode="save"):  
        if(mode=="save"):
            session.add(objToSave)
        session.commit()


def commit_change(
    object_to_save,
    name,
):
    session = get_session()
    try:
        commit_with_retry(object_to_save, session)
    except Exception as e:
        stacktrace = traceback.extract_stack()
        print(name, e, stacktrace)
    finally:
        if session:
            session.close()
            session = None


class CommitThread(threading.Thread):
    def __init__(
        self,
        object_to_save,
        name
    ):
        threading.Thread.__init__(self)
        self.object_to_save = object_to_save
        self.name = name

    def run(self):
        commit_change(self.object_to_save, self.name)



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

    '''def store_object(self, object_to_save):
        thread1 = CommitThread(object_to_save, "SESyntheticTestInputJson") 
        thread1.start()'''

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
                engine_1 = create_azsql_engine("SAMetadataOp")
                output_table = pd.read_sql(query, engine_1)

        return output_table
    
    def read_sql_to_df_prod(self, query, db_key):
        output_table = None
        if db_key is not None:
            logging.info(f"Database connection to {db_key}")
            if(db_key=="SAImplementation"):
                conn = get_connection_saimplementation()
            if(db_key=="SAMetadata"):
                conn = get_connection_sametadata()
            if(db_key=="SAOp"):
                conn = get_connection_saop_prd()
            output_table = pd.read_sql(query, conn)
        else:
            try:
                conn = get_connection_metadata_op()
                output_table = pd.read_sql(query, conn)
            except Exception as e:
                logging.info("Try the connection again" + str(e))
                engine_1 = create_azsql_engine("SAMetadataOp")
                output_table = pd.read_sql(query, engine_1)

        return output_table

    def read_sf_sql_to_df(self, query):
        # return pd.read_sql(query, sf_ctx())
        return pd.read_sql(query, create_sf_engine())

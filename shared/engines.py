import logging
import traceback
import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import *
from shared.common import *


class SQLEngine:
    def __init__(self) -> None:
        self.db_data, self.freq = self.get_config()
        self.engine_saop = create_azsql_engine(self.db_data,"SAOp")
        self.engine_sametadata = create_azsql_engine(self.db_data,"SAMetadata")

    def get_config(self):
        with open('local_settings.json') as file:
            config = json.load(file)

        self.db_data = config.get('Values')
        self.freq = config.get('Frequency')
        return self.db_data, self.freq

    def get_connection_sametadata(self):
        try: 
            if((self.engine_sametadata==None) or (self.engine_sametadata.engine.engine._connection_cls._is_disconnect)):  
                self.engine_sametadata = create_azsql_engine(self.db_data,"SAMetadata")
            return self.engine_sametadata
        except Exception as e:
            traceback.print_exc()

    def get_connection_saop(self):
        try: 
            if((self.engine_saop==None) or (self.engine_saop.engine.engine._connection_cls._is_disconnect)):  
                self.engine_saop = create_azsql_engine(self.db_data,"SAOp")
            return self.engine_saop
        except:
            traceback.print_exc()


class SQLReader(SQLEngine):
    def __init__(self):
        super().__init__()
        self.output_table = None

    def read_sql_to_df(self, query, db_key):
        
        if db_key is not None:
            logging.info(f"Database connection to {db_key}")
            if(db_key=="SAMetadata"):
                conn = self.get_connection_sametadata()
            if(db_key=="SAOp"):
                conn = self.get_connection_saop()
            self.output_table = pd.read_sql(query, conn)
        else:
            try:
                conn = self.get_connection_sametadata()
                self.output_table = pd.read_sql(query, conn)
            except Exception as e:
                logging.info("Try the connection again" + str(e))
                engine_1 = create_azsql_engine(self.db_data,"SAMetadataOp")
                self.output_table = pd.read_sql(query, engine_1)

        return self.output_table
    
    def read_sf_sql_to_df(self, query):
        return pd.read_sql(query, create_sf_engine(self.db_data))

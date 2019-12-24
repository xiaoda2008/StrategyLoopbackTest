'''
Created on 2019年11月3日

@author: xiaoda
'''
import pandas
import copy
import sqlalchemy

from sqlalchemy import Column, String,Integer,create_engine
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import mysqlURL
from pandas.core.frame import DataFrame
from sqlalchemy.orm import sessionmaker
import traceback

#from sqlalchemy.sql import and_,or_

class MysqlProcessor():
    
    def __init__(self):
        '''
        Constructor
        '''
        #print("init of MysqlProcessor()")
        self.engine=create_engine(mysqlURL)
        self.engine.connect()
        DBSession=sessionmaker(bind=self.engine)
        self.session=DBSession()
    
    def __del__(self):
        #print("del of MysqlProcessor()")
        self.session.close()
        self.engine.dispose()
        
            
    def getMysqlEngine(self):
        return self.engine
   
    def getMysqlSession(self):
        return self.session

    @staticmethod
    def execSql(session,sqlStr,cmtFlg=True):
        '''
        这里要注意一个问题，就是有可能执行了，但需要统一commit
        可以考虑并不commit
        而是将session返回，由调用者决定是否commit
        或者增加一个参数，决定是否commit
        如果参数为True，则直接commit
        如果参数为FALSE，则返回session，由调用者自主commit
        '''
        sqlStrTxt = sqlalchemy.text(sqlStr)
        
        #执行sql语句
        try:
            session.execute(sqlStrTxt)
        except:
            traceback.print_exc()
        finally:
            
            if cmtFlg:
                session.commit()
                
    

    def querySql(self,sqlStr):
        #session = MysqlProcessor.getMysqlSession()
        #mysqlEngine=MysqlProcessor.getMysqlEngine()
        sqlStrTxt=sqlalchemy.text(sqlStr)
        df=DataFrame()
        #执行sql语句
        try:
            #cursor=session.execute(sqlStrTxt)
            #result=cursor.fetchall()
            #df=pandas.DataFrame(list(result))
            df=pandas.read_sql_query(sqlStrTxt,self.engine)
        except:
            traceback.print_exc()
        finally:
            return df
'''
Created on 2019年11月3日

@author: xiaoda
'''
import sys
import pandas
from datetime import datetime as dt
import datetime
import copy
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String,Integer,create_engine
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import mysqlURL
from abc import abstractstaticmethod
from pandas.core.frame import DataFrame
from unittest.mock import inplace
from sqlalchemy.orm import sessionmaker
import traceback

#from sqlalchemy.sql import and_,or_

class MysqlProcessor():
    
    
    @staticmethod
    def getMysqlEngine():
        engine = create_engine(mysqlURL)
        return engine
   
    @staticmethod
    def getMysqlSession():
        mysqlEngine = create_engine(mysqlURL)
        #数据库回话
        DBSession= sessionmaker(bind=mysqlEngine)
        session= DBSession()
        return session

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
        #session = MysqlProcessor.getMysqlSession()
        sqlStrTxt = sqlalchemy.text(sqlStr)
        
        #执行sql语句
        try:
            session.execute(sqlStrTxt)
        except:
            traceback.print_exc()
        finally:
            
            if cmtFlg:
                session.commit()
                
    
    
    @staticmethod
    def querySql(sqlStr):
        #session = MysqlProcessor.getMysqlSession()
        mysqlEngine = MysqlProcessor.getMysqlEngine()
        sqlStrTxt = sqlalchemy.text(sqlStr)
        
        #执行sql语句
        try:
            #cursor=session.execute(sqlStrTxt)
            #result=cursor.fetchall()
            #df=pandas.DataFrame(list(result))
            df = pandas.read_sql_query(sqlStrTxt,mysqlEngine)
        except:
            traceback.print_exc()
        finally:
            return df
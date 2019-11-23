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
    def execSql(sqlStr):
        session = MysqlProcessor.getMysqlSession()
        sqlStrTxt = sqlalchemy.text(sqlStr)
        #执行sql语句
        try:
            session.execute(sqlStrTxt)
        except:
            traceback.print_exc()
        finally:
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
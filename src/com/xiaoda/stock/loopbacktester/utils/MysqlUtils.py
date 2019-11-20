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


#from sqlalchemy.sql import and_,or_

class MysqlProcessor():
    
    
    @staticmethod
    def getMysqlEngine():
        engine = create_engine(mysqlURL)
        return engine
    



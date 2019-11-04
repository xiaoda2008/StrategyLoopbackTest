'''
Created on 2019年11月3日

@author: xiaoda
'''
import sqlalchemy
from sqlalchemy import Column, String,Integer,create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from pytdx import trade
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import mysqlURL

class MysqlUtils():
    
    
    @staticmethod
    def getMysqlEngine():
        engine = create_engine(mysqlURL)
        return engine
    
    
    @staticmethod
    def isMarketDay(dtStr):
        mysqlEngine = MysqlUtils.getMysqlEngine()
        
        # 创建对象的基类:
        Base = declarative_base()

        # 定义User对象:
        class TradeCal(Base):
            # 表的名字:
            __tablename__ = 'u_trade_cal'

            # 表的结构:
            exchange = Column(String(20))
            cal_date = Column(String(20), primary_key=True)
            is_open = Column(Integer, primary_key=True)
        
            def __repr__(self):
                return self.name
        
        # 创建DBSession类型:
        DBSession = sessionmaker(bind=mysqlEngine)
        
        # 创建session对象:
        session = DBSession()
        
        tradeCal = session.query(TradeCal).filter(TradeCal.cal_date==dtStr).one()
        # 打印类型和对象的name属性:
        print('type:', type(tradeCal))
        print('cal_date:', tradeCal.cal_date)
        print('is_open:', tradeCal.is_open)
        # 关闭Session:
        session.close()
        
        if tradeCal.is_open==1:
            return True
        else:
            return False
    
'''    
    def getStockList(self):
        engine = self.getMysqlEngine()
        
        # 创建对象的基类:
        Base = declarative_base()

        # 定义User对象:
        class TradeCal(Base):
            # 表的名字:
            __tablename__ = 'u_trade_cal'

            # 表的结构:
            exchange = Column(String(20))
            cal_date = Column(String(20), primary_key=True)
            is_open = Column(Integer, primary_key=True)
        
            def __repr__(self):
                return self.name
        
        # 创建DBSession类型:
        DBSession = sessionmaker(bind=engine)
        
        # 创建session对象:
        session = DBSession()
        
        tradeCal = session.query(TradeCal).filter(TradeCal.cal_date==dtStr).one()
        # 打印类型和对象的name属性:
        print('type:', type(tradeCal))
        print('cal_date:', tradeCal.cal_date)
        print('is_open:', tradeCal.is_open)
        # 关闭Session:
        session.close()
        
        if tradeCal.is_open==1:
            return True
        else:
            return False
'''
'''
Created on 2019年11月3日

@author: xiaoda
'''
import pandas
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String,Integer,create_engine
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import mysqlURL

#from sqlalchemy.sql import and_,or_

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

        # 定义TradeCal对象:
        class TradeCal(Base):
            # 表的名字:
            __tablename__ = 'u_trade_cal'
            # 表的结构:
            exchange = Column(String(20))
            cal_date = Column(String(20), primary_key=True)
            is_open = Column(Integer)
        
            def __repr__(self):
                return self.cal_date
        
        # 创建DBSession类型:
        DBSession = sessionmaker(bind=mysqlEngine)
        
        # 创建session对象:
        session = DBSession()
        
        tradeCal = session.query(TradeCal).filter(TradeCal.cal_date==dtStr).one()

        # 关闭Session:
        session.close()
        
        if tradeCal.is_open==1:
            return True
        else:
            return False
    
    @staticmethod
    def getStockList():
        engine = MysqlUtils.getMysqlEngine()
        
        # 创建对象的基类:
        Base = declarative_base()

        # 定义StockList对象:
        class StockList(Base):
            # 表的名字:
            __tablename__ = 'u_stock_list'

            # 表的结构:
            ts_code=Column(String(20),primary_key=True)
            symbol=Column(String(20))
            name=Column(String(20))
            area=Column(String(20))
            industry=Column(String(20))
            list_date=Column(String(20))
        
            def __repr__(self):
                return self.ts_code
        
        # 创建DBSession类型:
        DBSession = sessionmaker(bind=engine)
        
        # 创建session对象:
        session = DBSession()
        
        stockList = session.query(StockList).all()

        # 关闭Session:
        session.close()
        
        return stockList

    @staticmethod
    def getStockKData(stockCode,startDate,endDate):
        engine = MysqlUtils.getMysqlEngine()
        #查询语句
        sql = 'select * from s_kdata_%s where trade_date>=%s and trade_date<=%s'%(stockCode,startDate,endDate)
        #查询结果
        df = pandas.read_sql_query(sql,engine)
        return df

        '''
        # 创建对象的基类:
        Base = declarative_base()
        
        # 定义StockKData对象:
        class StockKData(Base):
                
            # 表的名字:
            __tablename__ = 's_kdata_'+stockCode

            # 表的结构:
            ts_code=Column(String(20))
            trade_date=Column(String(20),primary_key=True)
            open=Column(Float)
            high=Column(Float)
            low=Column(Float)
            close=Column(Float)
            pre_close=Column(Float)
            change=Column(Float)
            pct_chg=Column(Float)
            vol=Column(Integer)
            amount=Column(Integer)
        
        
        # 创建DBSession类型:
        DBSession = sessionmaker(bind=engine)
        
        # 创建session对象:
        session = DBSession()
        
        stockKData = session.query(StockKData).filter(and_(StockKData.trade_date>=startDate,StockKData.trade_date<=endDate)).all()

        # 关闭Session:
        session.close()
        
        #stockKData为List，需要转换为DataFram返回，以适应数据处理逻辑
        if len(stockKData)==0:
            return None
        else:
            sData=DataFrame(stockKData)
            return sData
        '''    
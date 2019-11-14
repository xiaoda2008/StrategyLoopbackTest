'''
Created on 2019年11月3日

@author: xiaoda
'''
import pandas
from datetime import datetime as dt
import datetime
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String,Integer,create_engine
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import mysqlURL
from abc import abstractstaticmethod

#from sqlalchemy.sql import and_,or_

class MysqlProcessor():
    
    
    @staticmethod
    def getMysqlEngine():
        engine = create_engine(mysqlURL)
        return engine
    
    
    @staticmethod
    def getTradeCal():
        engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = "select * from u_trade_cal"
        #查询结果
        sqltxt = sqlalchemy.text(sql)
        df = pandas.read_sql_query(sqltxt,engine)
        return df
        
    
    @staticmethod
    def isMarketDay(dtStr):
        mysqlEngine = MysqlProcessor.getMysqlEngine()
        
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
    def getNextMarketDay(todayDate):
        '''
        找到下一个交易日，不含当天
        '''
        nextMarketDay=todayDate
        while True:
            #当前日期为节假日，查看下一天是否是交易日
            cday = dt.strptime(nextMarketDay, "%Y%m%d").date()
            dayOffset = datetime.timedelta(1)
            # 获取想要的日期的时间
            nextMarketDay = (cday+dayOffset).strftime('%Y%m%d')
            
            if MysqlProcessor.isMarketDay(dt.strptime(nextMarketDay,"%Y%m%d").date().strftime('%Y%m%d')):
                #找到第一个交易日，跳出
                break
        
        return nextMarketDay

    @staticmethod
    def getLastMarketDay(todayDate):
        '''
        找到上一个交易日，不含当天
        '''
        lastMarketDay=todayDate
        while True:
            #当前日期为节假日，查看下一天是否是交易日
            cday = dt.strptime(lastMarketDay, "%Y%m%d").date()
            dayOffset = datetime.timedelta(1)
            # 获取想要的日期的时间
            lastMarketDay = (cday-dayOffset).strftime('%Y%m%d')
            
            if MysqlProcessor.isMarketDay(dt.strptime(lastMarketDay,"%Y%m%d").date().strftime('%Y%m%d')):
                #找到第一个交易日，跳出
                break
        
        return lastMarketDay
    
    @staticmethod
    def getStockList():
        engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = "select * from u_stock_list where name not like '%ST%' and name not like '%退%'"
        #查询结果
        sqltxt = sqlalchemy.text(sql)
        df = pandas.read_sql_query(sqltxt,engine)
        return df
        
        '''
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
        '''

    @staticmethod
    def getStockKData(stockCode,startDate,endDate):
        engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = 'select * from s_kdata_%s where trade_date>=%s and trade_date<=%s order by trade_date'%(stockCode,startDate,endDate)
        #查询结果
        df = pandas.read_sql_query(sql,engine)
        return df


    @staticmethod
    def getLatestStockBalanceSheet(stockCode):
        engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = 'select * from s_balancesheet_%s order by ann_date desc limit 1;'%(stockCode[:6])
        #查询结果
        df = pandas.read_sql_query(sql,engine)
        return df
    
    @staticmethod
    def getLatestStockCashFlow(stockCode):
        engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = 'select * from s_cashflow_%s order by ann_date desc limit 1;'%(stockCode[:6])
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
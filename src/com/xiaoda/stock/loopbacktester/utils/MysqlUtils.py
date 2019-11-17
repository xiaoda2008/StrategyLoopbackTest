'''
Created on 2019年11月3日

@author: xiaoda
'''
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
        
        return df['ts_code'].to_list()
        
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
    def getStockKData(stockCode,startDate,endDate,adj):
        '''
        stockCode：股票代码
        startData：开始日期
        endDate：结束日期
        adj:None代表不复权，qfq代表前复权，hfq代表后复权
        '''
        engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql_kdata = 'select * from s_kdata_%s where trade_date>=%s and trade_date<=%s order by trade_date'%(stockCode[:6],startDate,endDate)
        sqltxt_kdata = sqlalchemy.text(sql_kdata)
        
        sql_adj = 'select * from s_adjdata_%s where trade_date>=%s and trade_date<=%s order by trade_date'%(stockCode[:6],startDate,endDate)
        sqltxt_adj = sqlalchemy.text(sql_adj)
        
        
        #查询结果
        try:
            kdatadf=pandas.read_sql_query(sqltxt_kdata,engine)
            adjdf=pandas.read_sql_query(sqltxt_adj,engine)
            #adjdf.set_index('trade_date',inplace=True)
            #kdatadf.set_index('trade_date',inplace=True)
            #获取到的数据都是未复权数据
            #这里需要对数据进行复权处理
            
            joineddf=pandas.merge(kdatadf, adjdf, on='trade_date', how='left')

            if adj=='qfq':
                #如果需要前复权数据
                #需要根据复权因子及k线数据进行计算
                latest_adj_factor=float(joineddf.at[joineddf.shape[0]-1,'adj_factor'])
                joineddf['open']=joineddf['open']*joineddf['adj_factor']/latest_adj_factor
                joineddf['high']=joineddf['high']*joineddf['adj_factor']/latest_adj_factor
                joineddf['low']=joineddf['low']*joineddf['adj_factor']/latest_adj_factor
                joineddf['close']=joineddf['close']*joineddf['adj_factor']/latest_adj_factor            
                joineddf['pre_close']=joineddf['pre_close']*joineddf['adj_factor']/latest_adj_factor            
                joineddf['change']=joineddf['change']*joineddf['adj_factor']/latest_adj_factor
            elif adj=='hfq':
                #如果需要后复权数据
                #需要根据复权因子及k线数据进行计算
                #kdatadf_hfq=copy.deepcopy(kdatadf)
                joineddf['open']=joineddf['open']*joineddf['adj_factor']
                joineddf['high']=joineddf['high']*joineddf['adj_factor']
                joineddf['low']=joineddf['low']*joineddf['adj_factor']
                joineddf['close']=joineddf['close']*joineddf['adj_factor']             
                joineddf['pre_close']=joineddf['pre_close']*joineddf['adj_factor']
                joineddf['change']=joineddf['change']*joineddf['adj_factor']
            else:
                pass
        except sqlalchemy.exc.ProgrammingError:
            #如果压根就没有这个表
            #在kdata与股票列表数据不一致的情况下会出现
            joineddf=DataFrame()
        finally:
            return joineddf


    @staticmethod
    def getLatestStockBalanceSheet(stockCode,dateStr):
        '''
        获取指定日期前最近一次的财务报表数据
        '''
        #startday=MysqlProcessor.getlastquarterfirstday().strftime('%Y%m%d')
        
        engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = 'select * from s_balancesheet_%s where total_assets is not null and ann_date<=%s order by ann_date desc limit 1;'%(stockCode[:6],dateStr)
        sqltxt = sqlalchemy.text(sql)
        #查询结果
        try:
            df=pandas.read_sql_query(sqltxt,engine)
        except sqlalchemy.exc.ProgrammingError:
            #如果压根就没有这个表
            #在kdata与股票列表数据不一致的情况下会出现
            df=DataFrame()
        finally:
        #获取到的数据都是未复权数据
        #这里需要对数据进行复权处理
            return df
        
    @staticmethod
    def getLatestStockCashFlow(stockCode,dateStr):
        '''
        获取指定日期前最近一次财务报表数据
        '''
        #startday=MysqlProcessor.getlastquarterfirstday().strftime('%Y%m%d')
        
        engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = 'select * from s_cashflow_%s where c_cash_equ_end_period is not null and ann_date<=%s order by ann_date desc limit 1;'%(stockCode[:6],dateStr)
        #查询结果
        sqltxt = sqlalchemy.text(sql)
        #查询结果
        try:
            df=pandas.read_sql_query(sqltxt,engine)
        except sqlalchemy.exc.ProgrammingError:
            #如果压根就没有这个表
            #在kdata与股票列表数据不一致的情况下会出现
            df=DataFrame()
        finally:
            return df
        #获取到的数据都是未复权数据
        #这里需要对数据进行复权处理

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
   
    '''
    @staticmethod
    def getlastquarterfirstday(dateStr):
        #today=dt.now()
        quarter = (dateStr[4:6]-1)/3+1
        if quarter == 1:
            return dt(dateStr[0:4]-1,10,1)
        elif quarter == 2:
            return dt(dateStr[0:4],1,1)
        elif quarter == 3:
            return dt(dateStr[0:4],4,1)
        else:
            return dt(dateStr[0:4],7,1)
    '''
'''
Created on 2019年11月18日

@author: xiaoda
'''
import tushare
import pandas
import sqlalchemy
import datetime
from datetime import datetime as dt
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String,Integer,create_engine
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from abc import abstractstaticmethod

class StockDataProcessor(object):
    '''
    classdocs
    '''


    def __init__(self, params):
        '''
        Constructor
        '''


    
    @staticmethod
    def getTradeCal():
        #engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = "select * from u_trade_cal"
        #查询结果
        return MysqlProcessor.querySql(sql)
        #df = pandas.read_sql_query(sqltxt,engine)
        #return df
    
    
    @staticmethod
    def isDealDay(dtStr):
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
    def getNextDealDay(todayDate,include):
        '''
        找到下一个交易日
        todayDate:起始计算的日期
        include:说明是否包含起始日期，
        '''
        
        if include==True and StockDataProcessor.isDealDay(todayDate):
            return todayDate
            
        nextDealDay=todayDate
        
        while True:
            #当前日期为节假日，查看下一天是否是交易日
            cday = dt.strptime(nextDealDay, "%Y%m%d").date()
            dayOffset = datetime.timedelta(1)
            # 获取想要的日期的时间
            nextDealDay = (cday+dayOffset).strftime('%Y%m%d')
            
            if StockDataProcessor.isDealDay(nextDealDay):
                #找到第一个交易日，跳出
                break
        
        return nextDealDay

    @staticmethod
    def getDealDayByOffset(todayDate,offset):
        trade_cal_data=StockDataProcessor.getTradeCal()
        trade_cal_data=trade_cal_data.set_index('cal_date')
        
        cday = dt.strptime(todayDate, "%Y%m%d").date()
        dayOffset = datetime.timedelta(1)
        cnt=0
        # 获取想要的日期的时间
        while True:
            cday = (cday - dayOffset)
            if trade_cal_data.at[cday.strftime('%Y%m%d'),'is_open']==1:
                cnt+=1
                if cnt==offset:
                    break
        return cday.strftime('%Y%m%d')
    
        
    @staticmethod
    def getLastDealDay(todayDate,include):
        '''
        找到上一个交易日
        todayDate:起始计算的日期
        include:说明是否包含起始日期，
        '''
        if include==True and StockDataProcessor.isDealDay(todayDate):
            return todayDate
        
        lastMarketDay=todayDate
        while True:
            #当前日期为节假日，查看下一天是否是交易日
            cday = dt.strptime(lastMarketDay, "%Y%m%d").date()
            dayOffset = datetime.timedelta(1)
            # 获取想要的日期的时间
            lastMarketDay = (cday-dayOffset).strftime('%Y%m%d')
            
            if StockDataProcessor.isDealDay(dt.strptime(lastMarketDay,"%Y%m%d").date().strftime('%Y%m%d')):
                #找到第一个交易日，跳出
                break
        
        return lastMarketDay
    
    @staticmethod
    def getAllStockDataDict():
        #engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = "select * from u_stock_list where name not like '%ST%' and name not like '%退%'"
        #查询结果
        #sqltxt = sqlalchemy.text(sql)
        #df = pandas.read_sql_query(sqltxt,engine)
        
        df=MysqlProcessor.querySql(sql)
        #return df['ts_code'].to_list()
        
        return df[['ts_code','list_date']].set_index('ts_code')['list_date'].to_dict()
    
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
        #engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql_kdata = 'select * from s_kdata_%s where trade_date>=%s and trade_date<=%s order by trade_date'%(stockCode[:6],startDate,endDate)
        #sqltxt_kdata = sqlalchemy.text(sql_kdata)
        
        sql_adj = 'select * from s_adjdata_%s where trade_date>=%s and trade_date<=%s order by trade_date'%(stockCode[:6],startDate,endDate)
        #sqltxt_adj = sqlalchemy.text(sql_adj)
        
        
        #查询结果
        try:
            kdatadf=MysqlProcessor.querySql(sql_kdata)
            #kdatadf=pandas.read_sql_query(sqltxt_kdata,engine,coerce_float=True)
            adjdf=MysqlProcessor.querySql(sql_adj)
            #adjdf=pandas.read_sql_query(sqltxt_adj,engine)
            #adjdf.set_index('trade_date',inplace=True)
            #kdatadf.set_index('trade_date',inplace=True)
            #获取到的数据都是未复权数据
            #这里需要对数据进行复权处理
            #kdatadf.dtypes
            
            kdatadf[['open','high','low','close','pre_close','change','pct_chg','vol','amount']]=\
            kdatadf[['open','high','low','close','pre_close','change','pct_chg','vol','amount']].astype(float)
            
            adjdf[['adj_factor']]=adjdf[['adj_factor']].astype(float)
            
            joineddf=pandas.merge(kdatadf, adjdf, on=['ts_code','trade_date'], how='left')
            #显示所有列
            #pandas.set_option('display.max_columns', None)
            #显示所有行
            #pandas.set_option('display.max_rows',None)
            #savedStdout = sys.stdout  #保存标准输出流
            #sys.stdout = open('d:/tstest.csv','wt+')
            #print(joineddf)
            #sys.stdout = savedStdout #恢复标准输出流
            #joineddf.dtypes
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

        except sqlalchemy.exc.ProgrammingError:
            #如果压根就没有这个表
            #在kdata与股票列表数据不一致的情况下会出现
            joineddf=pandas.DataFrame()
        finally:
            return joineddf
        
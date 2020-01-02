'''
Created on 2019年11月18日

@author: xiaoda
'''
import tushare
import pandas
import sqlalchemy
import datetime
from datetime import datetime as dt
from sqlalchemy import Column, String,Integer,create_engine
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor

class StockDataProcessor(object):
    '''
    实际运行中发现
    凡是从数据库取数，速度都会很慢
    当需要循环进行处理，每次都从数据库取数，效率非常低
    所以可以考虑对于日历、股票列表等，采用在初始化时候直接取到本地
    函数调用时候，只是从本地内存中取数进行处理，避免每次都到数据库取数的低效
    
    为解决这个问题
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.mysqlProcessor=MysqlProcessor()
        #engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = "select * from u_trade_cal"
        #查询结果
        self.tradeCalDF=self.mysqlProcessor.querySql(sql)
        self.tradeCalDF.set_index('cal_date',drop=True,inplace=True)


        #engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = "select * from u_stock_list where name not like '%ST%' and name not like '%退%' order by ts_code asc"
        
        df=self.mysqlProcessor.querySql(sql)
        #self.allStockDict=df[['ts_code','list_date']].set_index('ts_code')['list_date'].to_dict()
        self.allStockDict=df.set_index('ts_code').to_dict(orient="index")

        #查询语句
        sql = "select * from u_stock_list where HS300=1 and name not like '%ST%' and name not like '%退%' order by ts_code asc"
        
        df=self.mysqlProcessor.querySql(sql)

        self.hs300Dict=df.set_index('ts_code').to_dict(orient="index")

    
    def isDealDay(self,dtStr):
        if self.tradeCalDF.at[dtStr,'is_open']==1:
            return True
        else:
            return False
        
    @staticmethod
    def getNextCalDay(todayDate):
        '''
                找到下一个自然日
        '''
        cday=dt.strptime(todayDate, "%Y%m%d").date()
        dayOffset=datetime.timedelta(1)
        # 获取想要的日期的时间
        nextCalDay=(cday+dayOffset).strftime('%Y%m%d')
        return nextCalDay
    
    @staticmethod
    def getDateDistance(day1,day2):
        '''
        计算两个日期之间的时间间隔
        返回day2-day1
        如果为正值，说明day2晚于day1
        否则为负值
        '''
        cday1=dt.strptime(day1, "%Y%m%d").date()
        cday2=dt.strptime(day2, "%Y%m%d").date()
        interval=cday2-cday1
        
        return interval.days
    
    @staticmethod
    def getCalDayByOffset(todayDate,offset):
        '''
                找到距离当前日期向前offset的自然日
                负数表示向更早去找
                正数表示向更晚去找
        '''
        cday = dt.strptime(todayDate, "%Y%m%d").date()
        
        if offset>0:
            dayOffset=datetime.timedelta(1)
        else:
            dayOffset=datetime.timedelta(-1)
        
        cnt=0
        # 获取想要的日期的时间
        while True:
            cday=(cday+dayOffset)
            cnt+=1
            if cnt==abs(offset):
                break
        return cday.strftime('%Y%m%d') 

    def getNextDealDay(self,todayDate,include):
        '''
                找到下一个交易日
        todayDate:起始计算的日期
        include:说明是否包含起始日期，
        '''
        
        if include==True and self.isDealDay(todayDate):
            return todayDate
            
        nextDealDay=todayDate
        
        while True:
            #当前日期为节假日，查看下一天是否是交易日
            nextDealDay=StockDataProcessor.getNextCalDay(nextDealDay)
            
            if self.isDealDay(nextDealDay):
                #找到第一个交易日，跳出
                break
        
        return nextDealDay

    def getDealDayByOffset(self,todayDate,offset):
        '''
                找到距离当前日期向前offset的交易日
                负数表示向更早去找
                正数表示向更晚去找
        '''
        cday = dt.strptime(todayDate, "%Y%m%d").date()
        if offset>0:
            dayOffset=datetime.timedelta(1)
        else:
            dayOffset=datetime.timedelta(-1)
        
        cnt=0
        # 获取想要的日期的时间
        while True:
            cday=(cday+dayOffset)
            if self.tradeCalDF.at[cday.strftime('%Y%m%d'),'is_open']==1:
                cnt+=1
                if cnt==abs(offset):
                    break
        return cday.strftime('%Y%m%d')
    
    
    def getStockInfo(self,stockCode):
        #查询语句
        sql = "select * from u_stock_list where ts_code=\'%s\'"%(stockCode)
     
        #查询结果
        try:
            infodf=self.mysqlProcessor.querySql(sql)
        except sqlalchemy.exc.ProgrammingError:
            #如果压根就没有这个表
            #在kdata与股票列表数据不一致的情况下会出现
            infodf=pandas.DataFrame()
        finally:
            return infodf


    def getLastDealDay(self,todayDate,include):
        '''
        找到上一个交易日
        todayDate:起始计算的日期
        include:说明是否包含起始日期，
        '''
        if include==True and self.isDealDay(todayDate):
            return todayDate
        
        lastMarketDay=todayDate
        while True:
            #当前日期为节假日，查看下一天是否是交易日
            cday = dt.strptime(lastMarketDay, "%Y%m%d").date()
            dayOffset = datetime.timedelta(1)
            # 获取想要的日期的时间
            lastMarketDay = (cday-dayOffset).strftime('%Y%m%d')
            
            if self.isDealDay(lastMarketDay):
                #找到第一个交易日，跳出
                break
        
        return lastMarketDay
    
    
    def getAllStockDataDict(self):    
        return self.allStockDict
 

    
    def getHS300Dict(self):
        return self.hs300Dict 
 
    
    @staticmethod
    def mktallocation(stringx):
        if (stringx[:3] in ['000','600','603','601']) or (stringx in ['001696','001896','001979','001965']) :
            output = 'main'
        elif stringx[:3] == '002':
            output = 'SME'
        elif stringx[:3] == '300':
            output = 'GEM'
        elif stringx[:3] == '688':
            output = 'KCB'
        else:
            output = ''
            print('market allocation error:' + stringx)
        return output



    def preProcessKDataDF(self,stock_k_data):
        '''
        预处理Kdata，完成MA20等的计算
        '''
        pass
    
    def getStockKData(self,stockCode,startDate,endDate,adj):
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
            kdatadf=self.mysqlProcessor.querySql(sql_kdata)
            #kdatadf=pandas.read_sql_query(sqltxt_kdata,engine,coerce_float=True)
            adjdf=self.mysqlProcessor.querySql(sql_adj)
            #adjdf=pandas.read_sql_query(sqltxt_adj,engine)
            #adjdf.set_index('trade_date',inplace=True)
            #kdatadf.set_index('trade_date',inplace=True)
            #获取到的数据都是未复权数据
            #这里需要对数据进行复权处理
            #kdatadf.dtypes
            
            kdatadf[['open','high','low','close','pre_close','change','pct_chg','vol','amount']]=\
            kdatadf[['open','high','low','close','pre_close','change','pct_chg','vol','amount']].astype(float)
            
            adjdf[['adj_factor']]=adjdf[['adj_factor']].astype(float)
            
            joineddf=pandas.merge(kdatadf,adjdf,on=['ts_code','trade_date'],how='left')
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
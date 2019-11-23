'''
Created on 2019年11月2日

@author: xiaoda

一次性的获取所有需要的数据到本地数据库中
并在过程中进行处理情况的记录，以便kettle进行错误处理
'''

import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]

#print(sys.path)
#sys.path.clear()
print(sys.path)
sys.path.append('E:\workspace\StrategyLoopbackTest\src')
sys.path.append('D:\Programs\Python\Python37-32\Lib\site-packages')
sys.path.append('D:\Programs\Python\Python37-32')
sys.path.append('D:\Programs\Python\Python37-32\DLLs')
sys.path.append('D:\Programs\Python\Python37-32\lib')


import time
import tushare
import sqlalchemy
import datetime
from sqlalchemy.util.langhelpers import NoneType
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
#from com.xiaoda.stock.loopbacktester.utils.ParamUtils import LOGGINGDIR
from datetime import datetime as dt


log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')

def getlastquarterfirstday():
        today=dt.now()
        quarter = (today.month-1)/3+1
        if quarter == 1:
            return dt(today.year-1,10,1)
        elif quarter == 2:
            return dt(today.year,1,1)
        elif quarter == 3:
            return dt(today.year,4,1)
        else:
            return dt(today.year,7,1)
    

def partialUpdate():    
    #部分更新语句
    pupdatesql="update u_dataupdatelog set content='%s' where content_name='last_update_time';"%(dt.now().strftime('%Y%m%d'))
    MysqlProcessor.execSql(pupdatesql)

def totalUpdate():
    #全局更新语句
    tupdatesql="update u_dataupdatelog set content='%s' where content_name='last_total_update_time';"%(dt.now().strftime('%Y%m%d'))
    MysqlProcessor.execSql(tupdatesql)


def lastDataUpdate(stockCode,dataType):
    if dataType=='FR':
        #更新财务报表最新股票代码
        sql="update u_dataupdatelog set content='%s' where content_name='finance_report_update_to';"%(stockCode)
    elif dataType=="KD":
        #更新K线最新股票代码
        sql="update u_dataupdatelog set content='%s' where content_name='kdata_update_to';"%(stockCode)
    elif dataType=="ADJ":
        #更新复权因子最新股票代码
        sql="update u_dataupdatelog set content='%s' where content_name='adjdata_update_to';"%(stockCode)
    else:
        raise Exception("dataType不正确")
    
    MysqlProcessor.execSql(sql)
        

#使用TuShare pro版本

#写入数据库的引擎
mysqlEngine = MysqlProcessor.getMysqlEngine()

tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')

sdDataAPI = tushare.pro_api()


#1、获取交易日信息，并存入数据库
STARTDATE = '19990101'
ENDDATE = '20191231'

trade_cal_data = sdDataAPI.trade_cal(exchange='',start_date=STARTDATE,end_date=ENDDATE,fields='exchange,cal_date,is_open,pretrade_date')

#将交易日列表存入数据库表中
trade_cal_data.to_sql(name='u_trade_cal',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

#完成部分信息更新
partialUpdate()


#2、获取股票列表并存入数据库
sdf = sdDataAPI.stock_basic(exchange='',list_status='L',fields='ts_code,symbol,name,area,industry,list_date')

#将股票列表存入数据库表中
sdf.to_sql(name='u_stock_list',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

stockCodeList = sdf['ts_code']
#完成部分信息更新
partialUpdate()



#3、获取财务报表数据，存入数据库
#startday=getlastquarterfirstday().strftime('%Y%m%d')

#找到之前处理的最后一个股票的代码
sql="select content from u_dataupdatelog where content_name='finance_report_update_to'"
res=MysqlProcessor.querySql(sql)
finance_report_update_to=res.at[0,'content']


startday='19901219'

for index,stockCode in stockCodeList.items():

    if stockCode<finance_report_update_to:
        continue
    #if sdf.at[idx,'ts_code'][:6]<'000951':
    #    continue
    #elif sdf.at[idx,'ts_code'][:6]>'600428':
    #    break;
    
    #time.sleep(0.25)
    
    #获取资产负债表
    bs=sdDataAPI.balancesheet(ts_code=stockCode,start_date=startday,end_date=dt.now().strftime('%Y%m%d'))
    bs.to_sql(name='s_balancesheet_'+stockCode[:6],
              con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

    #获取现金流量表
    cf = sdDataAPI.cashflow(ts_code=stockCode,start_date=startday,end_date=dt.now().strftime('%Y%m%d'))
    cf.to_sql(name='s_cashflow_'+stockCode[:6],
              con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

    #获取利润表
    ic = sdDataAPI.income(ts_code=stockCode,start_date=startday,end_date=dt.now().strftime('%Y%m%d'))
    ic.to_sql(name='s_income_'+stockCode[:6],
              con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    
    log.logger.info('处理完%s的财务报表数据'%(stockCode))
    partialUpdate()
    lastDataUpdate(stockCode, "FR")
    

#4、获取股票不复权日K线数据，并存入数据库

#找到之前处理的最后一个股票的代码
sql="select content from u_dataupdatelog where content_name='kdata_update_to'"
res=MysqlProcessor.querySql(sql)
kdata_update_to=res.at[0,'content']

for index,stockCode in stockCodeList.items():
    
    if stockCode<kdata_update_to:
        continue
    
    #用于标记该股票是否出现过数据
    flag = False
    
    #将1990-01-01到19999-12-31该股票数据导入数据库
    STARTDATE = '19900101'
    ENDDATE = '19991231'
    
    #获取该股票数据并写入数据库
    stock_k_data=tushare.pro_bar(ts_code=stockCode, start_date=STARTDATE, end_date=ENDDATE)
    
    #time.sleep(0.5)
    
    #if type(stock_k_data)==NoneType:
    if stock_k_data.empty:
        #如果没有任何返回值，说明该时间段内没有上市交易过该股票
        log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,STARTDATE,ENDDATE))
                
        #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
        #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
        #time.sleep(0.31)
    else:
        #该股票出现交易数据，必然是第一次出现，直接替代即可
        flag = True
        stock_k_data.sort_index(inplace=True,ascending=False)
        #存入数据库
        stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)

    
    log.logger.info('处理完股票%s在%s到%s区间内的数据'%(stockCode,STARTDATE,ENDDATE))
    
    
    #将1999-01-01到2009-12-31该股票数据导入数据库
    STARTDATE = '19990101'
    ENDDATE = '20091231'
 
 
    #获取该股票数据并写入数据库
    stock_k_data = tushare.pro_bar(ts_code=stockCode, start_date=STARTDATE, end_date=ENDDATE)
    #time.sleep(0.5)
    
    if type(stock_k_data)==NoneType:
        #如果没有任何返回值，说明该时间段内没有上市交易过该股票
        log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,STARTDATE,ENDDATE))
                
        #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
        #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
        #time.sleep(0.31)
    elif flag == False:
        #该股票出现交易数据，且在上一区间未出现交易
        #则需要重建表
        flag = True
        stock_k_data.sort_index(inplace=True,ascending=False)
        #存入数据库
        stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)

    else:
        #该股票出现交易数据
        #且在上一区间已经有过交易，直接增加数据即可
        flag = True
        stock_k_data.sort_index(inplace=True,ascending=False)
        #存入数据库
        stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)

    log.logger.info('处理完股票%s在%s到%s区间内的数据'%(stockCode,STARTDATE,ENDDATE))

    #将2010-01-01到2018-12-31该股票数据导入数据库
    STARTDATE = '20100101'
    ENDDATE = '20181231'

    #获取该股票数据并写入数据库
    stock_k_data = tushare.pro_bar(ts_code=stockCode, start_date=STARTDATE, end_date=ENDDATE)
    #time.sleep(0.5)    
    if type(stock_k_data)==NoneType:
        #如果没有任何返回值，说明该时间段内没有上市交易过该股票
        log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,STARTDATE,ENDDATE))
                
        #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
        #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
        #time.sleep(0.31)
    elif flag == False:
        #该股票出现交易数据，且在上一区间未出现交易
        #则需要重建表
        flag = True
        stock_k_data.sort_index(inplace=True,ascending=False)
        #存入数据库
        stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
    else:
        #该股票出现交易数据
        #且在上一区间已经有过交易，直接增加数据即可
        flag = True
        stock_k_data.sort_index(inplace=True,ascending=False)
        #存入数据库
        stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
    
    log.logger.info('处理完股票%s在%s到%s区间内的数据'%(stockCode,STARTDATE,ENDDATE))
    
    #将2019-01-01到当前日期该股票数据导入数据库
    STARTDATE='20190101'
    ENDDATE=dt.now().strftime('%Y%m%d')

    #获取该股票数据并写入数据库
    stock_k_data = tushare.pro_bar(ts_code=stockCode, start_date=STARTDATE, end_date=ENDDATE)
    #time.sleep(0.5)    
    if type(stock_k_data)==NoneType:
        #如果没有任何返回值，说明该时间段内没有上市交易过该股票
        log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,STARTDATE,ENDDATE))
                
        #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
        #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
        #time.sleep(0.31)
    elif flag == False:
        #该股票出现交易数据，且在上一区间未出现交易
        #则需要重建表
        flag = True
        stock_k_data.sort_index(inplace=True,ascending=False)
        #存入数据库
        stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
    else:
        #该股票出现交易数据
        #且在上一区间已经有过交易，直接增加数据即可
        flag = True
        stock_k_data.sort_index(inplace=True,ascending=False)
        #存入数据库
        stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
    
    log.logger.info('处理完股票%s在%s到%s区间内的数据'%(stockCode,STARTDATE,ENDDATE))
    partialUpdate()
    lastDataUpdate(stockCode[:6], "KD")    
    
 
#5、获取复权因子数据，并存入数据库

#找到之前处理的最后一个股票的代码
sql="select content from u_dataupdatelog where content_name='adjdata_update_to'"
res=MysqlProcessor.querySql(sql)
adjdata_update_to=res.at[0,'content']

for index,stockCode in stockCodeList.items():

    if stockCode<adjdata_update_to:
        continue
    
    #time.sleep(0.5)
    #获取该股票的复权因子数据并写入数据库
    adj_data = sdDataAPI.adj_factor(ts_code=stockCode)
    
    adj_data.sort_index(inplace=True,ascending=False)
    
    #存入数据库
    adj_data.to_sql(name='s_adjdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)

    log.logger.info('处理完股票%s的复权因子'%(stockCode))

    partialUpdate()
    lastDataUpdate(stockCode[:6], "ADJ")  

   
#完成所有数据的更新
totalUpdate()


#完成所有数据更新，把数据库表重置，以便下一次处理使用
lastDataUpdate("", "FR")
lastDataUpdate("", "KD")
lastDataUpdate("", "ADJ")
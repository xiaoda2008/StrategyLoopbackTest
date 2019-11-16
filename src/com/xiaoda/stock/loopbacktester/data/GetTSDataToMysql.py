'''
Created on 2019年11月2日

@author: xiaoda
'''

import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]

print(sys.path)
sys.path.clear()
print(sys.path)
sys.path.append('E:\workspace\StrategyLoopbackTest\src')
sys.path.append('D:\Programs\Python\Python37-32\Lib\site-packages')
sys.path.append('D:\Programs\Python\Python37-32')
sys.path.append('D:\Programs\Python\Python37-32\DLLs')
sys.path.append('D:\Programs\Python\Python37-32\lib')


import os
import time
import tushare
from sqlalchemy.util.langhelpers import NoneType
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import LOGGINGDIR
from datetime import datetime as dt

log = Logger(LOGGINGDIR+'/'+os.path.split(__file__)[-1].split(".")[0] + '.log',level='info')

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



#2、获取股票列表并存入数据库
sdf = sdDataAPI.stock_basic(exchange='',list_status='L',fields='ts_code,symbol,name,area,industry,list_date')

#将股票列表存入数据库表中
sdf.to_sql(name='u_stock_list',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

stockCodeList = sdf['ts_code']

'''
#3、获取财务报表数据，存入数据库
#startday=getlastquarterfirstday().strftime('%Y%m%d')

startday='19901219'

for idx in sdf.index:

    if sdf.at[idx,'ts_code'][:6]<'600106':
        continue
    #elif sdf.at[idx,'ts_code'][:6]>'600428':
    #    break;
    
    #获取资产负债表
    bs=sdDataAPI.balancesheet(ts_code=sdf.at[idx,'ts_code'],start_date=startday,
                                end_date=dt.now().strftime('%Y%m%d'))
    #bs.at[0,'total_assets']
    #time.sleep(2)
    bs.to_sql(name='s_balancesheet_'+sdf.at[idx,'ts_code'][:6],
              con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)



    #获取现金流量表
    cf = sdDataAPI.cashflow(ts_code=sdf.at[idx,'ts_code'],start_date=startday,
                            end_date=dt.now().strftime('%Y%m%d'))#, period='20190930')
    #cf.at[0,'c_cash_equ_end_period']
    cf.to_sql(name='s_cashflow_'+sdf.at[idx,'ts_code'][:6],
              con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

    log.logger.info('处理完%s的财务报表数据'%(sdf.at[idx,'ts_code']))
    

'''

#不能这样处理，不同区间取到的前复权数据不同，会影像处理的准确性

#4、获取股票不复权日K线数据，并存入数据库
for index,stockCode in stockCodeList.items():
    
    #if stockCode<'600533':
    continue
    
    #用于标记该股票是否出现过数据
    flag = False
    
    #将1990-01-01到19999-12-31该股票数据导入数据库
    STARTDATE = '19900101'
    ENDDATE = '19991231'
    
    #获取该股票数据并写入数据库
    stock_k_data = tushare.pro_bar(ts_code=stockCode, start_date=STARTDATE, end_date=ENDDATE)
    
    time.sleep(2)
    
    if type(stock_k_data)==NoneType:
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
    time.sleep(2)
    
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

    #将2010-01-01到2009-12-31该股票数据导入数据库
    STARTDATE = '20100101'
    ENDDATE = '20191031'

    #获取该股票数据并写入数据库
    stock_k_data = tushare.pro_bar(ts_code=stockCode, start_date=STARTDATE, end_date=ENDDATE)
    time.sleep(2)    
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
    
    
 
#5、获取复权因子数据，并存入数据库
for index,stockCode in stockCodeList.items():

    if stockCode<'600243':
        continue
    time.sleep(1)
    #获取该股票的复权因子数据并写入数据库
    adj_data = sdDataAPI.adj_factor(ts_code=stockCode)
    
    #存入数据库
    adj_data.to_sql(name='s_adjdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)

    log.logger.info('处理完股票%s的复权因子'%(stockCode))
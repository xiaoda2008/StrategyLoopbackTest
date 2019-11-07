'''
Created on 2019年11月2日

@author: xiaoda
'''
import os
import time
import tushare
from sqlalchemy.util.langhelpers import NoneType
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import LOGGINGDIR


log = Logger(LOGGINGDIR+'/'+os.path.split(__file__)[-1].split(".")[0] + '.log',level='debug')


#使用TuShare pro版本

#写入数据库的引擎
mysqlEngine = MysqlProcessor.getMysqlEngine()

tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')

sdDataAPI = tushare.pro_api()


#1、获取交易日信息，并存入数据库
STARTDATE = '19990101'
ENDDATE = '20191231'

trade_cal_data = sdDataAPI.trade_cal(exchange='', start_date=STARTDATE, end_date=ENDDATE)

#将交易日列表存入数据库表中
trade_cal_data.to_sql(name='u_trade_cal', con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)



#2、获取股票列表病存入数据库
sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

#将股票列表存入数据库表中
sdf.to_sql(name='u_stock_list', con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)


stockCodeList = sdf['ts_code']



#不能这样处理，不同区间取到的前复权数据不同，会影像处理的准确性

#3、获取股票数据，并存入数据库
for index,stockCode in stockCodeList.items():
    #用于标记该股票是否出现过数据
    flag = False
    
    #将1999-01-01到2009-12-31该股票数据导入数据库
    STARTDATE = '19990101'
    ENDDATE = '20091231'
    
    log.logger.debug('处理股票%s在%s到%s区间内的数据'%(stockCode,STARTDATE,ENDDATE))
    
    #获取该股票数据并写入数据库
    stock_k_data = tushare.pro_bar(ts_code=stockCode, adj='qfq', start_date=STARTDATE, end_date=ENDDATE)
    
    if type(stock_k_data)==NoneType:
        #如果没有任何返回值，说明该时间段内没有上市交易过该股票
        log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,STARTDATE,ENDDATE))
                
        #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
        #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
        time.sleep(0.31)
    else:
        #该股票出现交易数据
        flag = True
        stock_k_data.sort_index(inplace=True,ascending=False)
        #存入数据库
        stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)


    #将2010-01-01到2009-12-31该股票数据导入数据库
    STARTDATE = '20100101'
    ENDDATE = '20191231'

    log.logger.debug('处理股票%s在%s到%s区间内的数据'%(stockCode,STARTDATE,ENDDATE))
    
    #获取该股票数据并写入数据库
    stock_k_data = tushare.pro_bar(ts_code=stockCode, adj='qfq', start_date=STARTDATE, end_date=ENDDATE)
    
    if type(stock_k_data)==NoneType:
        #如果没有任何返回值，说明该时间段内没有上市交易过该股票
        log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,STARTDATE,ENDDATE))
                
        #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
        #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
        time.sleep(0.31)
    elif flag == False:
        #该股票出现交易数据
        flag = True
        stock_k_data.sort_index(inplace=True,ascending=False)
        #存入数据库
        stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
    else:
        flag = True
        stock_k_data.sort_index(inplace=True,ascending=False)
        #存入数据库
        stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)



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
sys.path.append(r'E:\eclipse-workspace\StrategyLoopbackTest\src')
sys.path.append(r'D:\Program Files\Python38\Lib\site-packages')
sys.path.append(r'D:\Program Files\Python38')
sys.path.append(r'D:\Program Files\Python38\DLLs')
sys.path.append(r'D:\Program Files\Python38\libs')
sys.path.append(r'C:\Users\picc\eclipse-workspace\StrategyLoopbackTester\src')
sys.path.append(r'C:\Users\picc\AppData\Local\Programs\Python\Python36-32\Lib\site-packages')
sys.path.append(r'C:\Users\picc\AppData\Local\Programs\Python\Python36-32')



from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
import time
import tushare
import sqlalchemy
import datetime
from sqlalchemy.util.langhelpers import NoneType
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
#from com.xiaoda.stock.loopbacktester.utils.ParamUtils import LOGGINGDIR
from datetime import datetime as dt


mysqlProcessor=MysqlProcessor()

import argparse

DAYONE='19900101'
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
    

def partialUpdate(mysqlSession):    
    #部分更新语句
    pupdatesql="update u_data_desc set content='%s' where content_name='last_update_time';"%(dt.now().strftime('%Y%m%d %H:%M:%S'))
    MysqlProcessor.execSql(mysqlSession,pupdatesql,True)

def totalUpdate(mysqlSession,sdProcessor):
    #全局更新语句
    tupdatesql="update u_data_desc set content='%s' where content_name='last_total_update_time';"%(dt.now().strftime('%Y%m%d %H:%M:%S'))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    #从sd往后找到第一个交易日，含sd
    tupdatesql="update u_data_desc set content='%s' where content_name='data_start_dealday';"%(sdProcessor.getNextDealDay(sd, True))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    #从ed往前找到最后一个交易日，是否含ed需要根据当前时间是否已经完成该日交易
    #最好是每天取前一个交易日的数据，这样就不会存在当天日期是否已经可用的问题
    tupdatesql="update u_data_desc set content='%s' where content_name='data_end_dealday';"%(sdProcessor.getLastDealDay(ed,True))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    mysqlSession.commit()
    

def lastDataUpdate(mysqlSession,stockCode,dataType):
    if dataType=='FR_StockCode':
        #更新财务报表最新股票代码
        sql="update u_data_desc set content='%s' where content_name='finance_report_stockcode_update_to';"%(stockCode)
    elif dataType=='FR_Date':
        sql="update u_data_desc set content='%s' where content_name='finance_report_date_update_to';"%(dt.now().strftime('%Y%m%d'))
    elif dataType=="KD":
        #更新K线最新股票代码
        sql="update u_data_desc set content='%s' where content_name='kdata_update_to';"%(stockCode)
    elif dataType=="ADJ":
        #更新复权因子最新股票代码
        sql="update u_data_desc set content='%s' where content_name='adjdata_update_to';"%(stockCode)
    else:
        raise Exception("dataType不正确")
    
    MysqlProcessor.execSql(mysqlSession,sql,True)
        


#使用TuShare pro版本

mysqlProcessor=MysqlProcessor()

#写入数据库的引擎
mysqlEngine = mysqlProcessor.getMysqlEngine()
mysqlSession=mysqlProcessor.getMysqlSession()

tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')

sdDataAPI = tushare.pro_api()


#1、获取交易日信息，并存入数据库
startday=DAYONE

trade_cal_data=sdDataAPI.trade_cal(exchange='',start_date=startday,end_date=dt.now().strftime('%Y%m%d'),fields='exchange,cal_date,is_open,pretrade_date')

#将交易日列表存入数据库表中
trade_cal_data.to_sql(name='u_trade_cal',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

#完成部分信息更新
partialUpdate(mysqlSession)


#2、获取股票列表并存入数据库
sdf=sdDataAPI.stock_basic(exchange='',list_status='L',fields='ts_code,symbol,name,area,industry,list_date')

#将股票列表存入数据库表中
sdf.to_sql(name='u_stock_list',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

stockCodeList=sdf['ts_code']
#完成部分信息更新
partialUpdate(mysqlSession)



sdProcessor=StockDataProcessor()

# 创建命令行解析器句柄，并自定义描述信息
parser = argparse.ArgumentParser(description="test the argparse package")
# 定义必选参数 positionArg
# parser.add_argument("project_name")
# 定义可选参数module
#开始日期为19900101，即取全量数据
parser.add_argument("--startdate","-sd",type=str, default='19911219',help="Enter the start date")
# 定义可选参数module1
#结束日期默认为当前日期的前一个交易日（不含当天，以便解决当天可能还未完成交易的问题）
parser.add_argument("--enddate","-ed",type=str, default=sdProcessor.getLastDealDay(dt.now().strftime('%Y%m%d'),False),help="Enter the end date")
# 指定参数类型（默认是 str）
# parser.add_argument('x', type=int, help='test the type')
# 设置参数的可选范围
# parser.add_argument('--verbosity3', '-v3', type=str, choices=['one', 'two', 'three', 'four'], help='test choices')
# 设置参数默认值
# parser.add_argument('--verbosity4', '-v4', type=str, choices=['one', 'two', 'three'], default=1,help='test default value')
args = parser.parse_args()  # 返回一个命名空间
#print(args)
params = vars(args)  # 返回 args 的属性和属性值的字典
v1=[]

sd=''
ed=''

for k, v in params.items():
    if k=='startdate':
        sd=v
    elif k=='enddate':
        if v>=dt.now().strftime('%Y%m%d'):
            #调整一下日期，如果输入的结束日期等于或晚于当天
            #则取当天之前的一个交易日
            v=StockDataProcessor.getLastDealDay(dt.now().strftime('%Y%m%d'),False)
        ed=v

startday=sd
endday=ed



#3、获取财务报表数据，存入数据库
#startday=getlastquarterfirstday().strftime('%Y%m%d')

#找到之前处理的最后一个股票的代码
sql="select content from u_data_desc where content_name='finance_report_stockcode_update_to'"
res=mysqlProcessor.querySql(sql)
try:
    finance_report_update_to=res.at[0,'content']
except:
    finance_report_update_to=''

for index,stockCode in stockCodeList.items():

    if stockCode<=finance_report_update_to:
        continue
    
    #获取资产负债表
    bs=sdDataAPI.balancesheet(ts_code=stockCode,start_date=startday,end_date=endday)
    bs.to_sql(name='s_balancesheet_'+stockCode[:6],
              con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

    #获取现金流量表
    cf = sdDataAPI.cashflow(ts_code=stockCode,start_date=startday,end_date=endday)
    cf.to_sql(name='s_cashflow_'+stockCode[:6],
              con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

    #获取利润表
    ic = sdDataAPI.income(ts_code=stockCode,start_date=startday,end_date=endday)
    ic.to_sql(name='s_income_'+stockCode[:6],
              con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    
    log.logger.info('处理完%s的财务报表数据'%(stockCode))
    partialUpdate(mysqlSession)
    lastDataUpdate(mysqlSession,stockCode, "FR_StockCode")
    
    time.sleep(0.9)

lastDataUpdate(mysqlSession,stockCode, "FR_Date")    

#4、获取股票不复权日K线数据，并存入数据库

#找到之前处理的最后一个股票的代码
sql="select content from u_data_desc where content_name='kdata_update_to'"
res=mysqlProcessor.querySql(sql)
kdata_update_to=res.at[0,'content']

for index,stockCode in stockCodeList.items():
    
    if stockCode<=kdata_update_to:
        continue
    
    #用于标记该股票是否出现过数据
    flag = False
    
    #将1990-01-01到19999-12-31该股票数据导入数据库
    if startday>'19991231' or endday<'19900101':
        pass
    else:
        if startday<'19900101':
            tmpSTARTDATE='19900101'
        else:
            tmpSTARTDATE=startday
        if endday>'19991231':
            tmpENDDATE='19991231'
        else:
            tmpENDDATE=endday
        #STARTDATE='19900101'
        #ENDDATE='19991231'
        
        #获取该股票数据并写入数据库
        stock_k_data=tushare.pro_bar(ts_code=stockCode, start_date=tmpSTARTDATE, end_date=tmpENDDATE)
        
        if stock_k_data.empty:
            #如果没有任何返回值，说明该时间段内没有上市交易过该股票
            log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE))   
            #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
            #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
        else:
            #该股票出现交易数据，必然是第一次出现，直接替代即可
            flag = True
            stock_k_data.sort_index(inplace=True,ascending=False)
            #存入数据库
            stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
    
        log.logger.info('处理完股票%s在%s到%s区间内的数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
        
    
    if startday>'20091231' or endday<'19990101':
        pass
    else:
        if startday<'19990101':
            tmpSTARTDATE='19990101'
        else:
            tmpSTARTDATE=startday
        if endday>'20091231':
            tmpENDDATE='20091231'
        else:
            tmpENDDATE=endday
        
        #将1999-01-01到2009-12-31该股票数据导入数据库
        #STARTDATE = '19990101'
        #ENDDATE = '20091231'
     
        #获取该股票数据并写入数据库
        stock_k_data = tushare.pro_bar(ts_code=stockCode, start_date=tmpSTARTDATE, end_date=tmpENDDATE)

        if type(stock_k_data)==NoneType:
            #如果没有任何返回值，说明该时间段内没有上市交易过该股票
            log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                    
            #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
            #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
        elif flag==False:
            #该股票出现交易数据，且在上一区间未出现交易
            #则需要重建表
            flag=True
            stock_k_data.sort_index(inplace=True,ascending=False)
            #存入数据库
            stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
        else:
            #该股票出现交易数据
            #且在上一区间已经有过交易，直接增加数据即可
            stock_k_data.sort_index(inplace=True,ascending=False)
            #存入数据库
            stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
    
        log.logger.info('处理完股票%s在%s到%s区间内的数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
    

    if startday>'20181231' or endday<'20100101':
        pass
    else:
        if startday<'20100101':
            tmpSTARTDATE='20100101'
        else:
            tmpSTARTDATE=startday
        if endday>'20181231':
            tmpENDDATE='20181231'
        else:
            tmpENDDATE=endday
            
        #将2010-01-01到2018-12-31该股票数据导入数据库
        #STARTDATE = '20100101'
        #ENDDATE = '20181231'
    
        #获取该股票数据并写入数据库
        stock_k_data = tushare.pro_bar(ts_code=stockCode, start_date=tmpSTARTDATE, end_date=tmpENDDATE)   
        if type(stock_k_data)==NoneType:
            #如果没有任何返回值，说明该时间段内没有上市交易过该股票
            log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE))
            #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
            #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
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
            stock_k_data.sort_index(inplace=True,ascending=False)
            #存入数据库
            stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
        
        log.logger.info('处理完股票%s在%s到%s区间内的数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
    
    
    if startday>dt.now().strftime('%Y%m%d') or endday<'20190101':
        pass
    else:
        if startday<'20190101':
            tmpSTARTDATE='20190101'
        else:
            tmpSTARTDATE=startday
        if endday>dt.now().strftime('%Y%m%d'):
            tmpENDDATE=dt.now().strftime('%Y%m%d')
        else:
            tmpENDDATE=endday
        
        #将2019-01-01到当前日期该股票数据导入数据库
        #STARTDATE='20190101'
        #ENDDATE=dt.now().strftime('%Y%m%d')
    
        #获取该股票数据并写入数据库
        stock_k_data = tushare.pro_bar(ts_code=stockCode, start_date=tmpSTARTDATE, end_date=tmpENDDATE)
   
        if type(stock_k_data)==NoneType:
            #如果没有任何返回值，说明该时间段内没有上市交易过该股票
            log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE)) 
            #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
            #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对

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
            stock_k_data.sort_index(inplace=True,ascending=False)
            #存入数据库
            stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
        
        log.logger.info('处理完股票%s在%s到%s区间内的数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
    
    
    partialUpdate(mysqlSession)
    lastDataUpdate(mysqlSession,stockCode,"KD")    
        
    time.sleep(0.9)
 
#5、获取复权因子数据，并存入数据库

#找到之前处理的最后一个股票的代码
sql="select content from u_data_desc where content_name='adjdata_update_to'"
res=mysqlProcessor.querySql(sql)
adjdata_update_to=res.at[0,'content']

for index,stockCode in stockCodeList.items():

    if stockCode<=adjdata_update_to:
        continue
    
    #获取该股票的复权因子数据并写入数据库
    adj_data = sdDataAPI.adj_factor(ts_code=stockCode,start_date=startday,end_date=endday)
    
    adj_data.sort_index(inplace=True,ascending=False)
    
    #存入数据库
    adj_data.to_sql(name='s_adjdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)

    log.logger.info('处理完股票%s的复权因子'%(stockCode))

    partialUpdate(mysqlSession)
    lastDataUpdate(mysqlSession,stockCode, "ADJ")
    
    time.sleep(0.3)

   
#完成所有数据的更新
totalUpdate(mysqlSession,sdProcessor)


#完成所有数据更新，把数据库表重置，以便下一次处理使用
lastDataUpdate(mysqlSession,"","FR_StockCode")
lastDataUpdate(mysqlSession,"","KD")
lastDataUpdate(mysqlSession,"","ADJ")

mysqlSession.close()
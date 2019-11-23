'''
Created on 2019年11月2日

@author: xiaoda
将数据库中的数据更新到最新日期
从数据库中记录的上次更新日，更新到当前日期
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
import datetime
import datetime as dt
from io import StringIO
import tushare
import sqlalchemy
import pandas
from sqlalchemy.util.langhelpers import NoneType
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
#from com.xiaoda.stock.loopbacktester.utils.ParamUtils import LOGGINGDIR
from datetime import datetime as dt
from sqlalchemy.orm import sessionmaker
import traceback



DAYONE=19900101

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
    

#写入数据库的引擎
mysqlEngine = MysqlProcessor.getMysqlEngine()


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
        sql="update u_dataupdatelog set content='%s' where content_name='finance_report_update_to';"%(stockCode[:6])
    elif dataType=="KD":
        #更新K线最新股票代码
        sql="update u_dataupdatelog set content='%s' where content_name='kdata_update_to';"%(stockCode[:6])
    elif dataType=="ADJ":
        #更新复权因子最新股票代码
        sql="update u_dataupdatelog set content='%s' where content_name='adjdata_update_to';"%(stockCode[:6])
    else:
        raise Exception("dataType不正确")
    
    MysqlProcessor.execSql(sql)
        



#使用TuShare pro版本
#初始化tushare账号
tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
sdDataAPI = tushare.pro_api()




#1、获取交易日信息，并存入数据库
#交易日历数据相对简单，可以每次都全量获取

STARTDATE = DAYONE

ENDDATE=dt.now().strftime('%Y%m%d')

trade_cal_data = sdDataAPI.trade_cal(exchange='',start_date=STARTDATE,end_date=ENDDATE,fields='exchange,cal_date,is_open,pretrade_date')

#将交易日列表存入数据库表中
trade_cal_data.to_sql(name='u_trade_cal',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)


log.logger.info('处理完交易日数据的更新')
#完成部分信息更新
partialUpdate()


#2、获取股票列表并存入数据库
#股票列表数据相对简单，可以每次都全量获取

sdf = sdDataAPI.stock_basic(exchange='',list_status='L',fields='ts_code,symbol,name,area,industry,list_date')

#将股票列表存入数据库表中
sdf.to_sql(name='u_stock_list',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

stockCodeList = sdf['ts_code']

log.logger.info('处理完股票列表数据的更新')
#完成部分信息更新
partialUpdate()


#3、获取财务报表数据，存入数据库

#查询语句
#查询上次完整获取数据的日期

sql = "select content from u_dataupdatelog where content_name='last_total_update_time'"
res=MysqlProcessor.querySql(sql)


if res.empty:
    startday=DAYONE
else:
    res.at[0,'content']
    cday = dt.strptime(res.at[0,'content'], "%Y%m%d").date()
    startday=(cday+datetime.timedelta(1)).strftime('%Y%m%d')


for index,stockCode in stockCodeList.items():

    #if sdf.at[idx,'ts_code'][:6]<'600106':
    #    continue
    #elif sdf.at[idx,'ts_code'][:6]>'600428':
    #    break;
    
    time.sleep(0.75)
    #获取资产负债表
    bs=sdDataAPI.balancesheet(ts_code=stockCode,start_date=startday,end_date=dt.now().strftime('%Y%m%d'))
    #获取现金流量表
    cf = sdDataAPI.cashflow(ts_code=stockCode,start_date=startday,end_date=dt.now().strftime('%Y%m%d'))
    #获取利润表
    ic = sdDataAPI.income(ts_code=stockCode,start_date=startday,end_date=dt.now().strftime('%Y%m%d'))
    
    if bs.empty or cf.empty or ic.empty:
        continue
    
    
    #由于是更新数据，绝大多数的表在此前都已经存在
    #因此，不能直接用dataframe的to_sql写入，否则会删除原有数据，或者可能重复插入
    #因此，需要生成sql代码进行插入
    
    #但对于确实没有数据的股票：刚刚上市，或者之前还没有发不过去财务报表等的，应当还是先用to_sql建立表格
    
    sql = "select table_name from information_schema.tables where table_name='s_balancesheet_%s'"%(stockCode[:6])
    res=MysqlProcessor.querySql(sql)
    #如果数据库中还没有这个表，需要建立表格
    if res.empty:
        bs.to_sql(name='s_balancesheet_'+stockCode[:6],
                  con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    else:
        #数据库中已经有这个表了
        #对balancesheet表中的数据，生成sql插入语句
        for bidx in bs.index:
            valStr=bs[bidx:bidx+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
            valList=valStr.split(',')
            paramStr=''
            for val in valList:
                if val=='NULL':
                    paramStr=paramStr+'NULL,'
                else:       
                    paramStr=paramStr+"'"+val+"'"+','
            paramStr=paramStr[:-1]
            sql='insert into s_balancesheet_%s values (%s)'%(stockCode[:6],paramStr)
            MysqlProcessor.execSql(sql)



    sql = "select table_name from information_schema.tables where table_name='s_cashflow_%s'"%(stockCode[:6])
    res=MysqlProcessor.querySql(sql)
    #如果数据库中还没有这个表，需要建立表格
    if res.empty:    
        cf.to_sql(name='s_cashflow_'+stockCode[:6],
                  con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    else:
        #数据库中已经有这个表了
        #对cashflow表中的数据，生成sql插入语句
        for cidx in cf.index:
            valStr=cf[cidx:cidx+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
            valList=valStr.split(',')
            paramStr=''
            for val in valList:
                if val=='NULL':
                    paramStr=paramStr+'NULL,'
                else:       
                    paramStr=paramStr+"'"+val+"'"+','
            paramStr=paramStr[:-1]
            sql='insert into s_cashflow_%s values (%s)'%(stockCode[:6],paramStr)
            MysqlProcessor.execSql(sql)
    
    
    
    sql = "select table_name from information_schema.tables where table_name='s_income_%s'"%(stockCode[:6])
    res=MysqlProcessor.querySql(sql)
    #如果数据库中还没有这个表，需要建立表格
    if res.empty:     
        ic.to_sql(name='s_income_'+stockCode[:6],
                  con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    else:
        #数据库中已经有这个表了
        #对income表中的数据，生成sql插入语句
        for iidx in ic.index:
            valStr=ic[iidx:iidx+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
            valList=valStr.split(',')
            paramStr=''
            for val in valList:
                if val=='NULL':
                    paramStr=paramStr+'NULL,'
                else:       
                    paramStr=paramStr+"'"+val+"'"+','
            paramStr=paramStr[:-1]
            sql='insert into s_income_%s values (%s)'%(stockCode[:6],paramStr)
            MysqlProcessor.execSql(sql)
    

    log.logger.info('处理完%s的财务报表数据'%(stockCode))
    
    partialUpdate()
    lastDataUpdate(stockCode, "FR")





STARTDATE=startday
ENDDATE=dt.now().strftime('%Y%m%d')
#4、获取股票不复权日K线数据，并存入数据库
for index,stockCode in stockCodeList.items():
    
    #if stockCode<'600533':
    #continue
    
    #将startday1到当前日期该股票数据导入数据库

    
    #获取该股票数据并写入数据库
    sk = tushare.pro_bar(ts_code=stockCode, start_date=STARTDATE, end_date=ENDDATE)
    
    if sk.empty:
        #如果没有任何返回值，说明该时间段内该股票没有交易数据
        log.logger.warning('%s在%s到%s时段内无交易数据'%(stockCode,STARTDATE,ENDDATE))
        continue

    sk.sort_index(inplace=True,ascending=False)
    
    sql = "select table_name from information_schema.tables where table_name='s_kdata_%s'"%(stockCode[:6])
    res=MysqlProcessor.querySql(sql)
    #如果数据库中还没有这个表，需要建立表格
    if res.empty:   
        #存入数据库
        sk.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
    else:
        #如果已经有该表，需要生成sql，插入表格
        #对stock_k_data表中的数据，生成sql插入语句
        for sidx in sk.index:
            valStr=sk[sidx:sidx+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
            valList=valStr.split(',')
            paramStr=''
            for val in valList:
                if val=='NULL':
                    paramStr=paramStr+'NULL,'
                else:       
                    paramStr=paramStr+"'"+val+"'"+','
            paramStr=paramStr[:-1]
            sql='insert into s_kdata_%s values (%s)'%(stockCode[:6],paramStr)
            MysqlProcessor.execSql(sql)
    
    log.logger.info('处理完股票%s在%s到%s区间内的数据'%(stockCode,STARTDATE,ENDDATE))
    
    partialUpdate()
    lastDataUpdate(stockCode, "KD")  
    
STARTDATE=startday
ENDDATE=dt.now().strftime('%Y%m%d') 
#5、获取复权因子数据，并存入数据库
for index,stockCode in stockCodeList.items():

    #获取该股票的复权因子数据并写入数据库
    ad = sdDataAPI.adj_factor(ts_code=stockCode,start_date=STARTDATE,end_date=ENDDATE)
    
    
    if ad.empty:
        continue
    
    ad.sort_index(inplace=True,ascending=False)
    
    
    sql = "select table_name from information_schema.tables where table_name='s_adjdata_%s'"%(stockCode[:6])
    res=MysqlProcessor.querySql(sql)
    #如果数据库中还没有这个表，需要建立表格
    if res.empty:
        #存入数据库
        ad.to_sql(name='s_adjdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
    else:
        for aidx in ad.index:
            valStr=ad[aidx:aidx+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
            valList=valStr.split(',')
            paramStr=''
            for val in valList:
                if val=='NULL':
                    paramStr=paramStr+'NULL,'
                else:       
                    paramStr=paramStr+"'"+val+"'"+','
            paramStr=paramStr[:-1]
            sql='insert into s_adjdata_%s values (%s)'%(stockCode[:6],paramStr)
            MysqlProcessor.execSql(sql)            
    log.logger.info('处理完股票%s的复权因子'%(stockCode))
    
    partialUpdate()
    lastDataUpdate(stockCode, "ADJ")  


#完成所有数据的更新
totalUpdate()

#完成所有数据更新，把数据库表重置，以便下一次处理使用
lastDataUpdate("", "FR")
lastDataUpdate("", "KD")
lastDataUpdate("", "ADJ")
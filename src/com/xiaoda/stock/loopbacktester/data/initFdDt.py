'''
Created on 2020年11月20日

@author: xiaoda

一次性的获取所有公募基金数据到本地数据库中
并在过程中进行处理情况的记录，以便kettle进行错误处理
'''

import sys
import os

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
import traceback
#print(sys.path)
#sys.path.clear()
#print(sys.path)
sys.path.append(r'E:\eclipse-workspace\StrategyLoopbackTest\src')
sys.path.append(r'D:\Program Files\Python38\Lib\site-packages')
sys.path.append(r'D:\Program Files\Python38')
sys.path.append(r'D:\Program Files\Python38\DLLs')
sys.path.append(r'D:\Program Files\Python38\libs')
sys.path.append(r'C:\Users\picc\eclipse-workspace\StrategyLoopbackTester\src')
sys.path.append(r'C:\Users\picc\AppData\Local\Programs\Python\Python36-32\Lib\site-packages')
sys.path.append(r'C:\Users\picc\AppData\Local\Programs\Python\Python36-32')


from com.xiaoda.stock.loopbacktester.utils.ParamUtils import fdMysqlURL
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
import tushare
from sqlalchemy.util.langhelpers import NoneType
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
#from com.xiaoda.stock.loopbacktester.utils.ParamUtils import LOGGINGDIR
from datetime import datetime as dt
import multiprocessing
from multiprocessing import Manager
import sqlalchemy
import argparse
import time


log=Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')

def get8slListFromFundList(fundList):
    retSLList=[]
    slLen=len(fundList)
    #对所有基金进行遍历
    i=0
    tmpList1=[]
    tmpList2=[]
    tmpList3=[]
    tmpList4=[]
    tmpList5=[]
    tmpList6=[]
    tmpList7=[]
    tmpList8=[]
    
    for fundCode in fundList:
        #log.logger.info(stockCode)
        if i<slLen*1/8:
            tmpList1.append(fundCode)
        elif i>=slLen*1/8 and i<slLen*2/8:
            tmpList2.append(fundCode)
        elif i>=slLen*2/8 and i<slLen*3/8:
            tmpList3.append(fundCode)
        elif i>=slLen*3/8 and i<slLen*4/8:
            tmpList4.append(fundCode)
        elif i>=slLen*4/8 and i<slLen*5/8:
            tmpList5.append(fundCode)
        elif i>=slLen*5/8 and i<slLen*6/8:
            tmpList6.append(fundCode)
        elif i>=slLen*6/8 and i<slLen*7/8:
            tmpList7.append(fundCode)
        else:
            tmpList8.append(fundCode)            
        i=i+1
    retSLList.append(tmpList1)
    retSLList.append(tmpList2)
    retSLList.append(tmpList3)
    retSLList.append(tmpList4)
    retSLList.append(tmpList5)
    retSLList.append(tmpList6)
    retSLList.append(tmpList7)
    retSLList.append(tmpList8)
                
    return retSLList 



def totalUpdate(mysqlSession,sdProcessor):
    #全局更新语句
    tupdatesql="update u_data_desc set content='%s' where content_name='last_total_update_time';"%(dt.now().strftime('%Y%m%d %H:%M:%S'))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    tupdatesql="update u_data_desc set content='%s' where content_name='finance_report_date_update_to';"%(dt.now().strftime('%Y%m%d'))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    #从sd往后找到第一个交易日，含sd
    tupdatesql="update u_data_desc set content='%s' where content_name='data_start_dealday';"%(sdProcessor.getNextDealDay(sd, True))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    #从ed往前找到最后一个交易日，是否含ed需要根据当前时间是否已经完成该日交易
    #最好是每天取前一个交易日的数据，这样就不会存在当天日期是否已经可用的问题
    tupdatesql="update u_data_desc set content='%s' where content_name='data_end_dealday';"%(sdProcessor.getLastDealDay(ed,True))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    mysqlSession.commit()
    
    


def processFundDataGet(fundList,startday,endday):

    #使用TuShare pro版本    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    sdDataAPI = tushare.pro_api()
    #写入数据库的引擎    
    mysqlProcessor=MysqlProcessor(fdMysqlURL)
    mysqlEngine=mysqlProcessor.getMysqlEngine()
    mysqlSession=mysqlProcessor.getMysqlSession()
    
        
    ix=0
    length=len(fundList)
    
    #场外基金
    #ts_fdListDF=fdDataAPI.fund_basic(market='O')
    #ts_fdList=list(ts_fdListDF['ts_code'])
    

    while ix<length:
       
        fundCode=fundList[ix]

        try:
            """
            #首先先根据wind获取的基金代码，到tushare中获取基金信息
            #获取信息后，将原有信息删除，替换为新获取的信息
            #由于fund_basic接口每分钟只能调用10次，因此该数据获取没有多进程的必要性
            #后续每天更新过程中，该部分内容则不需要更新，只需要按天更新基金净值、基金分红、复权因子等数据
            #对于在ts表中没有的基金，单独查询基金信息并写入

            sqlStr="delete from u_fund_list where ts_code='%s'"%(fundCode);
            mysqlProcessor.execSql(mysqlSession, sqlStr, True)
                        
            if fundCode not in ts_fdList:
                #如果ts表中没有该基金，需要单独查询基金信息并写入
                fund_basic_data=sdDataAPI.fund_basic(ts_code=fundCode)
                fund_basic_data.to_sql(name='u_fund_list',con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
                time.sleep(6)
                log.logger.info('已补充%s基金的信息'%(fundCode))
            else:
                time.sleep(0.9)
                log.logger.info('%s基金的信息不需要补充'%(fundCode))
            """                
            
            #基金净值
            fund_Nav=sdDataAPI.fund_nav(ts_code=fundCode)
            fund_Nav.to_sql(name='f_fund_nav_'+fundCode,
                      con=mysqlEngine,chunksize=100,if_exists='replace',index=None)
            
            
            '''
            #日内行情，日内行情没意义？
            fund_Nav=sdDataAPI.fund_daily(ts_code=fundCode, start_date=startday, end_date=endday)
            fund_Nav.to_sql(name='f_fund_daily_'+fundCode,
                      con=mysqlEngine,chunksize=100,if_exists='replace',index=None)
            '''
            
            #复权因子，貌似只有场内基金有复权因子，场外没有，场外只有分红？
            fund_Nav=sdDataAPI.fund_adj(ts_code=fundCode, start_date=startday, end_date=endday)
            fund_Nav.to_sql(name='f_fund_adj_'+fundCode,
                      con=mysqlEngine,chunksize=100,if_exists='replace',index=None) 
            
            #基金分红数据
            #写入单一的表，不要每个基金一张表
            fund_Nav=sdDataAPI.fund_div(ts_code=fundCode)
            fund_Nav.to_sql(name='u_fund_div',con=mysqlEngine,chunksize=100,if_exists='append',index=None)             

            #基金经理
            #基金经理的信息，是否应该写入单一的表，而不是每个基金一张表?
            fund_Nav=sdDataAPI.fund_manager(ts_code=fundCode)
            fund_Nav.to_sql(name='u_fund_mgr',con=mysqlEngine,chunksize=100,if_exists='append',index=None)
        except Exception:#出现异常
            #出现异常，则还需要继续循环，继续对该股票继续处理
            traceback.print_exc()
            log.logger.warning('处理%s的数据时出现异常，重新进行处理'%(fundCode))
            time.sleep(60)
            continue
        else:#未出现异常
            log.logger.info('%d处理完%s的净值、日线行情、复权因子数据'%(ix,fundCode))
            time.sleep(6)

            #未出现异常，进行下一个股票的处理
            ix=ix+1
    
    #到最后，把所有ts查到的基金信息先写进表中
    ts_fdListDF.to_sql(name='u_fund_list',con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
    
    
    
     
if __name__ == '__main__':
 
    #print("test")
    #写入数据库的引擎    
    mysqlProcessor=MysqlProcessor(fdMysqlURL)
    mysqlEngine=mysqlProcessor.getMysqlEngine()
    mysqlSession=mysqlProcessor.getMysqlSession()
        
    #使用TuShare pro版本    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    fdDataAPI = tushare.pro_api()

    #1、获取公募基金列表，并存入数据库
    #由于Tushare的基金获取需要5000以上积分才能获取全量
    #因此，从wind获取基金清单，存入到基金列表中
    #然后基于这个列表再到tushare获取各个基金的信息
    
    
    #场外基金
    fund_list_data_O=fdDataAPI.fund_basic(market='O') 
    #场内基金
    fund_list_data_E=fdDataAPI.fund_basic(market='E')
    fund_list_data=fund_list_data_E.append(fund_list_data_O)
    #将场内公募基金列表存入数据库表中
    fund_list_data.to_sql(name='u_fund_list',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    


    """
    #2、获取指数信息
    
    #沪深300指数
    indexDF=sdDataAPI.index_daily(ts_code='000300.SH',start_date=startday,end_date=dt.now().strftime('%Y%m%d'))
    
    #将指数数据存入数据库表中
    indexDF.to_sql(name='u_idx_hs300',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

    sqlStr='alter table u_idx_hs300 modify column trade_date varchar(20) primary key;'

    try:
        mysqlProcessor.execSql(mysqlSession,sqlStr,True)       
    except sqlalchemy.exc.OperationalError:
        log.logger.warning("修正HS300指数表出错，可能已经修正过")

    #创业板指数
    indexDF=sdDataAPI.index_daily(ts_code='399006.SZ',start_date=startday,end_date=dt.now().strftime('%Y%m%d'))
    
    #将指数数据存入数据库表中
    indexDF.to_sql(name='u_idx_cyb',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

    sqlStr='alter table u_idx_cyb modify column trade_date varchar(20) primary key;'

    try:
        mysqlProcessor.execSql(mysqlSession,sqlStr,True)       
    except sqlalchemy.exc.OperationalError:
        log.logger.warning("修正创业板指数表出错，可能已经修正过")

    
    #2、获取股票列表并存入数据库
    sdf=sdDataAPI.stock_basic(exchange='',list_status='L',fields='ts_code,symbol,name,area,industry,list_date')
    
    try:
        #将股票列表存入数据库表中
        sdf.to_sql(name='u_stock_list',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    except sqlalchemy.exc.IntegrityError:
        log.logger.warning("股票列表获取出现错误，可能已经获取过")
        
    stockCodeList=list(sdf['ts_code'])
    #完成部分信息更新
    #partialUpdate(mysqlSession)
    
    sqlStr='alter table u_stock_list modify column ts_code varchar(20) primary key;'
    sqlStr+='alter table u_stock_list add column HS300 tinyint(1) not null default 0;'
    sqlStr+='alter table u_stock_list add column SH50 tinyint(1) not null default 0;'
    sqlStr+='alter table u_stock_list add column SZ100 tinyint(1) not null default 0;'
    sqlStr+='alter table u_stock_list add column ZZ500 tinyint(1) not null default 0;'
    sqlStr+='alter table u_stock_list add column selfselected tinyint(1) not null default 0;'

    try:
        mysqlProcessor.execSql(mysqlSession,sqlStr,True)       
    except sqlalchemy.exc.OperationalError:
        log.logger.warning("修正股票清单表出错，可能已经修正过")




    '''
    sqlStr1="create table u_data_desc (content_name varchar(100),content varchar(200) not null default '',comments varchar(300) not null default '');"
    sqlStr2="insert into u_data_desc (content_name,content,comments) values ('last_total_update_time','','the time of last total data update');"
    sqlStr3="insert into u_data_desc (content_name,content,comments) values ('data_start_dealday','','the start deal day(include) of all data');"
    sqlStr4="insert into u_data_desc (content_name,content,comments) values ('data_end_dealday','','the end deal day(include) of all data');"
    sqlStr5="insert into u_data_desc (content_name,content,comments) values ('finance_report_date_update_to','','the date of last update of finance report');"


    try:
        mysqlProcessor.execSql(mysqlSession,sqlStr1,True)       
        mysqlProcessor.execSql(mysqlSession,sqlStr2,True)       
        mysqlProcessor.execSql(mysqlSession,sqlStr3,True)       
        mysqlProcessor.execSql(mysqlSession,sqlStr4,True)       
        mysqlProcessor.execSql(mysqlSession,sqlStr5,True)       
     
    except sqlalchemy.exc.OperationalError:
        log.logger.warning("初始化u_data_desc表出错")
    '''
    
    #sqlStr='alter table u_stock_list add column is_data_available tinyint(1) default 0;'
    #try:
    #    mysqlProcessor.execSql(mysqlSession,sqlStr,True)       
    #except sqlalchemy.exc.OperationalError:
    #    log.logger.warning("修正股票清单表出错，可能已经修正过")



    """
     
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
    
    #fund_list_data=mysqlProcessor.querySql('select * from u_fund_list')
    
    fundCodeList=list(fund_list_data['ts_code'])
    
    
    
    fdList=get8slListFromFundList(fundCodeList)
    
    #分8个进程，分别计算8段股票波动率
    process=[]

    for subStockList in fdList:
        
        p=multiprocessing.Process(target=processFundDataGet,args=(subStockList,startday,endday))
        p.start()
        process.append(p)
    
    for p in process:
        p.join()               
    
    
    #单进程
    #processFundDataGet(fundCodeList,startday,endday)
    
       
    
       
    #完成所有数据的更新
    totalUpdate(mysqlSession,sdProcessor)
    
        
    mysqlSession.close()
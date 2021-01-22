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
            #基金净值
            fund_Nav=sdDataAPI.fund_nav(ts_code=fundCode)
            fund_Nav.to_sql(name='f_fund_nav_'+fundCode[:-3],
                      con=mysqlEngine,chunksize=100,if_exists='replace',index=None)
            
            '''
            #日内行情，日内行情没意义？
            fund_Daily=sdDataAPI.fund_daily(ts_code=fundCode, start_date=startday, end_date=endday)
            fund_Daily.to_sql(name='f_fund_daily_'+fundCode[:-3],
                      con=mysqlEngine,chunksize=100,if_exists='replace',index=None)
            '''
            
            #复权因子，貌似只有场内基金有复权因子，场外没有，场外只有分红？
            fund_Adj=sdDataAPI.fund_adj(ts_code=fundCode, start_date=startday, end_date=endday)
            fund_Adj.to_sql(name='f_fund_adj_'+fundCode[:-3],
                      con=mysqlEngine,chunksize=100,if_exists='replace',index=None) 
            
            #基金分红数据
            #写入单一的表，不要每个基金一张表
            fund_Div=sdDataAPI.fund_div(ts_code=fundCode)
            fund_Div.to_sql(name='u_fund_div',con=mysqlEngine,chunksize=100,if_exists='append',index=None)             

            #基金经理
            #基金经理的信息，是否应该写入单一的表，而不是每个基金一张表?
            fund_Mgr=sdDataAPI.fund_manager(ts_code=fundCode)
            fund_Mgr.to_sql(name='u_fund_mgr',con=mysqlEngine,chunksize=100,if_exists='append',index=None)

            #基金持仓信息
            #基金持仓信息，是否应该写入单一的表，而不是每个基金一张表?
            fund_fund_portfolio=sdDataAPI.fund_portfolio(ts_code=fundCode)
            fund_fund_portfolio.to_sql(name='u_fund_portfolio',con=mysqlEngine,chunksize=100,if_exists='append',index=None)
            
                    
        except Exception:#出现异常
            #出现异常，则还需要继续循环，继续对该股票继续处理
            traceback.print_exc()
            log.logger.warning('%d：处理%s的数据时出现异常，重新进行处理'%(ix,fundCode))
            time.sleep(60)
            continue
        else:#未出现异常
            log.logger.info('%d：处理完%s的净值、日线行情、复权因子数据'%(ix,fundCode))
            time.sleep(8)
            #未出现异常，进行下一个股票的处理
            ix=ix+1
    
     
if __name__ == '__main__':
 
    #print("test")
    #写入数据库的引擎    
    mysqlProcessor=MysqlProcessor(fdMysqlURL)
    mysqlEngine=mysqlProcessor.getMysqlEngine()
    mysqlSession=mysqlProcessor.getMysqlSession()

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


    #使用TuShare pro版本    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    fdDataAPI = tushare.pro_api()

    #1、获取公募基金列表，并存入数据库
    #由于Tushare的基金获取需要5000以上积分才能获取全量
    #因此，从wind获取基金清单，存入到基金列表中
    #然后基于这个列表再到tushare获取各个基金的信息
     
    #场外基金
    tsFundListDF_O=fdDataAPI.fund_basic(market='O') 
    #场内基金
    tsFundListDF_E=fdDataAPI.fund_basic(market='E')
    tsFundListDF=tsFundListDF_E.append(tsFundListDF_O)
    #将公募基金列表存入数据库表中
    tsFundListDF.to_sql(name='u_fund_list',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    
    #Tushare获取的基金列表
    tsFundList=list(tsFundListDF['ts_code'])
    
    #wind获取的基金列表
    windFundListDF=mysqlProcessor.querySql('select * from u_wind_fund_list')
    windFundList=list(windFundListDF['ts_code'])
    
    #求两个列表的差集，在wind的基金列表，但不在tushare的基金列表中的部分
    #先把这部分基金的资料增加到基金列表中
    difFdList=list(set(windFundList).difference(set(tsFundList)))
      
    ix=0
    length=len(difFdList)
    
    
    
    while ix<length:
        fundCode=difFdList[ix]
        try:
            #查询基金信息并写入
            fund_basic_data=fdDataAPI.fund_basic(ts_code=fundCode)
            if len(fund_basic_data)>0:
                fund_basic_data.to_sql(name='u_fund_list',con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
            else:
                log.logger.error("%d：%s基金的基本信息无法获取"%(ix,fundCode))
            #time.sleep(6)
        except Exception:#出现异常
            #出现异常，则还需要继续循环，继续对该股票继续处理
            traceback.print_exc()
            log.logger.warning('%d：单独处理%s基金基本信息插入出错，重新进行处理'%(ix,fundCode))
            time.sleep(60)
            continue
        else:#未出现异常
            log.logger.info('%d：单独处理%s基金基本信息插入完毕'%(ix,fundCode))
            time.sleep(6)
            #未出现异常，进行下一个股票的处理
            ix=ix+1

    
    #单进程
    #processFundDataGet(fundCodeList,startday,endday)
    

    #上面已经获取了所有基金的基本资料
    #下面取获取所有基金交易信息
    wholeFundListDF=mysqlProcessor.querySql('select * from u_fund_list')   
    
    wholeFundLists=wholeFundListDF['ts_code']
    
    #先用并集基金的所有净值、分红等信息获取回来
    subWholeFdLists=get8slListFromFundList(wholeFundLists)
    #分8个进程
    process=[]

    for subWholeFdList in subWholeFdLists:
        
        p=multiprocessing.Process(target=processFundDataGet,args=(subWholeFdList,startday,endday))
        p.start()
        process.append(p)
    
    for p in process:
        p.join()               

           
    
       
    #完成所有数据的更新
    totalUpdate(mysqlSession,sdProcessor)
    
        
    mysqlSession.close()
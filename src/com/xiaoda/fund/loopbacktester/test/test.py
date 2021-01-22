'''
Created on 2020年11月29日

@author: xiaoda
'''

from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import fdMysqlURL
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
import math
import sys
import os
from _ast import If




if __name__ == '__main__':
     
    log=Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
     
    #对所有基金经理的清单
    #分析其所有管理的基金期间内
    #获取的收益，以及年化收益
    #写入数据库的引擎    
    mysqlProcessor=MysqlProcessor(fdMysqlURL)
    mysqlEngine=mysqlProcessor.getMysqlEngine()    
    
    fdCode="002148.OF"
    
    sqlStr="SELECT\
    distinct end_date,\
    adj_nav\
    FROM\
    f_fund_nav_%s\
    ORDER BY\
    end_date;"%(fdCode[:-3])

    
    fdNavDF=mysqlProcessor.querySql(sqlStr)

    startday=fdNavDF.at[0,'end_date']
    
    fdNavDF.set_index('end_date',drop=True,inplace=True)

    sdProcessor=StockDataProcessor()    

    sdf=mysqlProcessor.querySql('select content from u_data_desc where content_name=\'data_end_dealday\'')

    enddate=sdf.at[0,'content']
    
    ix=0
    
    currday=startday
    
    while ix<len(fdNavDF):
        
        
        try:    
            adjNav=fdNavDF.at[currday,'adj_nav']
            lastAdjNav=adjNav
            ix=ix+1
            
        except KeyError:
            
            adjNav=lastAdjNav


        print("%d:%s,净值:%f"%(ix,currday,adjNav))
        
        
        currday=sdProcessor.getNextCalDay(currday)
        
        if currday>enddate:
            break




 
    
    sqlStr="SELECT DISTINCT\
        ( mgr.NAME ) mgrName,\
        mgr.gender,\
        mgr.resume,\
        mgr.begin_date,\
        mgr.end_date,\
        list.ts_code,\
        list.NAME fundName,\
        list.management \
        FROM\
            u_fund_mgr mgr,\
            u_fund_list list \
        WHERE\
            mgr.ts_code = list.ts_code \
            and list.market='O'\
            and list.fund_type not like '%货币%'\
            and list.fund_type not like '%债%'\
            and list.issue_date <> ''\
        ORDER BY\
            mgr.NAME,\
            mgr.gender,\
            mgr.resume,\
            mgr.begin_date,\
            list.ts_code"
    fdMgrDF=mysqlProcessor.querySql(sqlStr)
       
    #print(len(fdMgrDF))
    
    mgrNo=len(fdMgrDF)
    
    ix=0
    
    
    sdf=mysqlProcessor.querySql('select content from u_data_desc where content_name=\'data_end_dealday\'')
    end_dealday=sdf.at[0,'content']
  
    sdProcessor=StockDataProcessor()




    
    

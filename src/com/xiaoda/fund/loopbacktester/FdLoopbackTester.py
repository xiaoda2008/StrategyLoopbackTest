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


def anaFdPerfDuringPeriod(tsCode,bgDate,edDate):
    
    #分析2010年
    sqlStr="select * from f_fund_nav_%s where end_date>'%s' and end_date<'%s' order by end_date"%(tsCode[:-3],bgDate,edDate)
    
    #获取该基金经理在任期间的基金净值情况
    fdNavDF=mysqlProcessor.querySql(sqlStr)

    if len(fdNavDF)>0:
        stAdjNav=fdNavDF.at[0,'adj_nav']
        edAdjNav=fdNavDF.at[len(fdNavDF)-1,'adj_nav']
          
        incRate=(edAdjNav-stAdjNav)/stAdjNav
        return incRate
    else:
        return 0



if __name__ == '__main__':
     
    log=Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
     
    #对所有基金经理的清单
    #分析其所有管理的基金期间内
    #获取的收益，以及年化收益
    #写入数据库的引擎    
    mysqlProcessor=MysqlProcessor(fdMysqlURL)
    mysqlEngine=mysqlProcessor.getMysqlEngine()    
    
    
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




    #分析所有基金经理在任职期间的表现
    savedStdout=sys.stdout  #保存标准输出流
    sys.stdout=open('d:/fdMgr.csv','wt+',newline='',encoding='utf-8-sig')            
    
    print('序号,基金经理,基金代码,基金名称,起始日期,结束日期,任职年数,累计收益,复合年化收益')
            
    while(ix<mgrNo):
        
        mgrName=fdMgrDF.at[ix,'mgrName']
        tsCode=fdMgrDF.at[ix,'ts_code']
        fdName=fdMgrDF.at[ix,'fundName']
        bgDate=fdMgrDF.at[ix,'begin_date']
        edDate=fdMgrDF.at[ix,'end_date']
        
        if edDate==None:
            edDate=end_dealday
            
        
        sqlStr="select * from f_fund_nav_%s where end_date>'%s' and end_date<'%s' order by end_date"%(tsCode[:-3],bgDate,edDate)
        
        #获取该基金经理在任期间的基金净值情况
        fdNavDF=mysqlProcessor.querySql(sqlStr)
           
        if len(fdNavDF)>0:

            stAdjNav=fdNavDF.at[0,'adj_nav']
            
            edAdjNav=fdNavDF.at[len(fdNavDF)-1,'adj_nav']
              
            incRate=(edAdjNav-stAdjNav)/stAdjNav
            
            dtDistAnn=sdProcessor.getDateDistance(bgDate,edDate)/365
            
            annRet=round(math.pow(1+incRate,1/dtDistAnn)-1,4)
                     
            print("%d,%s,%s,%s,%s,%s,%f,%f%%,%f%%"%(ix,mgrName,tsCode,fdName,bgDate,edDate,\
                                                                         round(dtDistAnn,2),round(incRate*100,2),\
                                                                         round(annRet*100,2)))
        else:
            print("%d,%s,%s,%s,%s,%s,%f,%f%%,%f%%"%(ix,mgrName,tsCode,fdName,bgDate,edDate,\
                                                                         round(dtDistAnn,2),0,0))
        
        
        log.logger.info('%d：处理完%s基金经理在%s到%s之间在%s基金的数据'%(ix,mgrName,bgDate,edDate,tsCode))
        
        ix=ix+1
        
        
        
        
    
    #分析所有基金经理在近10年内的表现情况    
    ix=0



    #获取沪深300指数、创业板指数的上涨情况
    #输出到表中，用于判断基金的收益是否超越了指数
      
    savedStdout=sys.stdout  #保存标准输出流
    sys.stdout=open('d:/perfAna.csv','wt+',newline='',encoding='utf-8-sig')            
    
    print('序号,基金经理,基金代码,基金名称,2010期间收益,2011期间收益,2012期间收益,2013期间收益,\
    2014期间收益,2015期间收益,2016期间收益,2017期间收益,2018期间收益,2019期间收益,2020期间收益')

    while(ix<mgrNo):
        
        mgrName=fdMgrDF.at[ix,'mgrName']
        tsCode=fdMgrDF.at[ix,'ts_code']
        fdName=fdMgrDF.at[ix,'fundName']
        bgDate=fdMgrDF.at[ix,'begin_date']
        edDate=fdMgrDF.at[ix,'end_date']
        
        if bgDate==None:
            bgDate=end_dealday
                    
        if edDate==None:
            edDate=end_dealday

        print("%d,%s,%s,%s"%(ix,mgrName,tsCode,fdName),end=',')
        
        if bgDate<='20100101' and edDate>='20101231':
            incRate=round(anaFdPerfDuringPeriod(tsCode, '20100101', '20101231')*100,2)
            print('%f%%'%(incRate),end=',')
        else:
            print('-',end=',')
            
        if bgDate<='20110101' and edDate>='20111231':
            incRate=round(anaFdPerfDuringPeriod(tsCode, '20110101', '20111231')*100,2)
            print('%f%%'%(incRate),end=',')
        else:
            print('-',end=',')
            
        if bgDate<='20120101' and edDate>='20121231':
            incRate=round(anaFdPerfDuringPeriod(tsCode, '20120101', '20121231')*100,2)
            print('%f%%'%(incRate),end=',')
        else:
            print('-',end=',')
            
        if bgDate<='20130101' and edDate>='20131231':
            incRate=round(anaFdPerfDuringPeriod(tsCode, '20130101', '20131231')*100,2)
            print('%f%%'%(incRate),end=',')
        else:
            print('-',end=',')
            
        if bgDate<='20140101' and edDate>='20141231':
            incRate=round(anaFdPerfDuringPeriod(tsCode, '20140101', '20141231')*100,2)
            print('%f%%'%(incRate),end=',')
        else:
            print('-',end=',')
            
        if bgDate<='20150101' and edDate>='20151231':
            incRate=round(anaFdPerfDuringPeriod(tsCode, '20150101', '20151231')*100,2)
            print('%f%%'%(incRate),end=',')
        else:
            print('-',end=',')
            
        if bgDate<='20160101' and edDate>='20161231':
            incRate=round(anaFdPerfDuringPeriod(tsCode, '20160101', '20161231')*100,2)
            print('%f%%'%(incRate),end=',')
        else:
            print('-',end=',')
            
        if bgDate<='20170101' and edDate>='20171231':
            incRate=round(anaFdPerfDuringPeriod(tsCode, '20170101', '20171231')*100,2)
            print('%f%%'%(incRate),end=',')
        else:
            print('-',end=',')
               
        if bgDate<='20180101' and edDate>='20181231':
            incRate=round(anaFdPerfDuringPeriod(tsCode, '20180101', '20181231')*100,2)
            print('%f%%'%(incRate),end=',')
        else:
            print('-',end=',')
            
        if bgDate<='20190101' and edDate>='20191231':
            incRate=round(anaFdPerfDuringPeriod(tsCode, '20190101', '20191231')*100,2)
            print('%f%%'%(incRate),end=',')
        else:
            print('-',end=',')
            
        if bgDate<='20200101' and edDate>=end_dealday:
            incRate=round(anaFdPerfDuringPeriod(tsCode, '20200101', end_dealday)*100,2)
            print('%f%%'%(incRate),end=',')
        else:
            print('-',end=',')       
    
        print()
    
        log.logger.info('%d：处理完%s基金经理最近十年在%s基金的数据'%(ix,mgrName,tsCode))    
    
        ix=ix+1
                    
    sys.stdout = savedStdout #恢复标准输出流
    
    
    
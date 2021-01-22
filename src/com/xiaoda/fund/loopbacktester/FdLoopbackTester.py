'''
Created on 2020年11月29日

@author: xiaoda
'''

from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import fdMysqlURL
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import tsmysqlURL
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
import math
import sys
import os


def anaFdPerfDuringPeriod(mysqlProcessor,tsCode,bgDate,edDate):
    
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
     
     
    
    
    #分析一：
    #对所有基金经理的清单，分析其所有管理的基金期间内
    #获取的收益，以及年化收益
    fdMysqlProcessor=MysqlProcessor(fdMysqlURL)  
    
    
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
            and list.fund_type not like '%货币%'\
            and list.fund_type not like '%债%'\
            and list.issue_date <> ''\
        ORDER BY\
            mgr.NAME,\
            mgr.gender,\
            mgr.resume,\
            mgr.begin_date,\
            list.ts_code"
    fdMgrDF=fdMysqlProcessor.querySql(sqlStr)
       
    #print(len(fdMgrDF))
    
    mgrNo=len(fdMgrDF)
    
    ix=0
      
    sdf=fdMysqlProcessor.querySql('select content from u_data_desc where content_name=\'data_end_dealday\'')
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
        fdNavDF=fdMysqlProcessor.querySql(sqlStr)
           
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
        
                        
    sys.stdout = savedStdout #恢复标准输出流
        
        
        
    #分析二：
    #分析所有基金经理在近10年内的表现情况    
    ix=0

    sdf=fdMysqlProcessor.querySql('select content from u_data_desc where content_name=\'data_end_dealday\'')
    lastDealDay=sdf.at[0,'content']

    #获取沪深300指数、创业板指数的上涨情况
    #输出到表中，用于判断基金的收益是否超越了指数
  
    tsMysqlProcessor=MysqlProcessor(tsmysqlURL)
        
    sqlStr="select * from u_idx_hs300 order by trade_date"
    hs300DF=tsMysqlProcessor.querySql(sqlStr)
    #计算2011-2020年之间每年涨幅
    hs300DF.set_index('trade_date',drop=True,inplace=True)
    
    sd=sdProcessor.getNextDealDay('20110101', True)
    ed=sdProcessor.getLastDealDay('20111231', True)
    si=hs300DF.at[sd,'open']
    ei=hs300DF.at[ed,'close']
    #2011年涨幅
    hs300ir2011=round(ei/si-1,4)
    
    sd=sdProcessor.getNextDealDay('20120101', True)
    ed=sdProcessor.getLastDealDay('20121231', True)
    si=hs300DF.at[sd,'open']
    ei=hs300DF.at[ed,'close']
    #2012年涨幅
    hs300ir2012=round(ei/si-1,4)

    
    sd=sdProcessor.getNextDealDay('20130101', True)
    ed=sdProcessor.getLastDealDay('20131231', True)
    si=hs300DF.at[sd,'open']
    ei=hs300DF.at[ed,'close']
    #2013年涨幅
    hs300ir2013=round(ei/si-1,4)


    sd=sdProcessor.getNextDealDay('20140101', True)
    ed=sdProcessor.getLastDealDay('20141231', True)
    si=hs300DF.at[sd,'open']
    ei=hs300DF.at[ed,'close']
    #2014年涨幅
    hs300ir2014=round(ei/si-1,4)
    
    sd=sdProcessor.getNextDealDay('20150101', True)
    ed=sdProcessor.getLastDealDay('20151231', True)
    si=hs300DF.at[sd,'open']
    ei=hs300DF.at[ed,'close']
    #2015年涨幅
    hs300ir2015=round(ei/si-1,4)    
        
    sd=sdProcessor.getNextDealDay('20160101', True)
    ed=sdProcessor.getLastDealDay('20161231', True)
    si=hs300DF.at[sd,'open']
    ei=hs300DF.at[ed,'close']
    #2016年涨幅
    hs300ir2016=round(ei/si-1,4)    
        
    sd=sdProcessor.getNextDealDay('20170101', True)
    ed=sdProcessor.getLastDealDay('20171231', True)
    si=hs300DF.at[sd,'open']
    ei=hs300DF.at[ed,'close']
    #2017年涨幅
    hs300ir2017=round(ei/si-1,4)

    sd=sdProcessor.getNextDealDay('20180101', True)
    ed=sdProcessor.getLastDealDay('20181231', True)
    si=hs300DF.at[sd,'open']
    ei=hs300DF.at[ed,'close']
    #2018年涨幅
    hs300ir2018=round(ei/si-1,4)

    sd=sdProcessor.getNextDealDay('20190101', True)
    ed=sdProcessor.getLastDealDay('20191231', True)
    si=hs300DF.at[sd,'open']
    ei=hs300DF.at[ed,'close']
    #2019年涨幅
    hs300ir2019=round(ei/si-1,4)

    sd=sdProcessor.getNextDealDay('20200101', True)
    ed=sdProcessor.getLastDealDay('20201231', True)
    si=hs300DF.at[sd,'open']
    ei=hs300DF.at[ed,'close']
    #2020年涨幅
    hs300ir2020=round(ei/si-1,4)
 
    sd=sdProcessor.getNextDealDay('20210101', True)
    ed=lastDealDay
    si=hs300DF.at[sd,'open']
    ei=hs300DF.at[ed,'close']
    #2021年涨幅
    hs300ir2021=round(ei/si-1,4)
  
  
  
    sqlStr="select * from u_idx_cyb order by trade_date"
    cybDF=tsMysqlProcessor.querySql(sqlStr)    
    
    cybDF.set_index('trade_date',drop=True,inplace=True)
    
    #计算2011-2020年之间每年涨幅
    sd=sdProcessor.getNextDealDay('20110101', True)
    ed=sdProcessor.getLastDealDay('20111231', True)
    si=cybDF.at[sd,'open']
    ei=cybDF.at[ed,'close']
    #2011年涨幅
    cybir2011=round(ei/si-1,4)
    
    sd=sdProcessor.getNextDealDay('20120101', True)
    ed=sdProcessor.getLastDealDay('20121231', True)
    si=cybDF.at[sd,'open']
    ei=cybDF.at[ed,'close']
    #2012年涨幅
    cybir2012=round(ei/si-1,4)

    
    sd=sdProcessor.getNextDealDay('20130101', True)
    ed=sdProcessor.getLastDealDay('20131231', True)
    si=cybDF.at[sd,'open']
    ei=cybDF.at[ed,'close']
    #2013年涨幅
    cybir2013=round(ei/si-1,4)


    sd=sdProcessor.getNextDealDay('20140101', True)
    ed=sdProcessor.getLastDealDay('20141231', True)
    si=cybDF.at[sd,'open']
    ei=cybDF.at[ed,'close']
    #2014年涨幅
    cybir2014=round(ei/si-1,4)
    
    sd=sdProcessor.getNextDealDay('20150101', True)
    ed=sdProcessor.getLastDealDay('20151231', True)
    si=cybDF.at[sd,'open']
    ei=cybDF.at[ed,'close']
    #2015年涨幅
    cybir2015=round(ei/si-1,4)    
        
    sd=sdProcessor.getNextDealDay('20160101', True)
    ed=sdProcessor.getLastDealDay('20161231', True)
    si=cybDF.at[sd,'open']
    ei=cybDF.at[ed,'close']
    #2016年涨幅
    cybir2016=round(ei/si-1,4)    
        
    sd=sdProcessor.getNextDealDay('20170101', True)
    ed=sdProcessor.getLastDealDay('20171231', True)
    si=cybDF.at[sd,'open']
    ei=cybDF.at[ed,'close']
    #2017年涨幅
    cybir2017=round(ei/si-1,4)

    sd=sdProcessor.getNextDealDay('20180101', True)
    ed=sdProcessor.getLastDealDay('20181231', True)
    si=cybDF.at[sd,'open']
    ei=cybDF.at[ed,'close']
    #2018年涨幅
    cybir2018=round(ei/si-1,4)

    sd=sdProcessor.getNextDealDay('20190101', True)
    ed=sdProcessor.getLastDealDay('20191231', True)
    si=cybDF.at[sd,'open']
    ei=cybDF.at[ed,'close']
    #2019年涨幅
    cybir2019=round(ei/si-1,4)

    sd=sdProcessor.getNextDealDay('20200101', True)
    ed=sdProcessor.getLastDealDay('20201231', True)
    si=cybDF.at[sd,'open']
    ei=cybDF.at[ed,'close']
    #2020年涨幅
    cybir2020=round(ei/si-1,4) 
 
    sd=sdProcessor.getNextDealDay('20210101', True)
    ed=lastDealDay
    si=cybDF.at[sd,'open']
    ei=cybDF.at[ed,'close']
    #2021年涨幅
    cybir2021=round(ei/si-1,4) 
     
    
    
      
    savedStdout=sys.stdout  #保存标准输出流
    sys.stdout=open('d:/mgrPerfAna.csv','wt+',newline='',encoding='utf-8-sig')            
    
    print('序号,基金经理,基金代码,基金名称,\
    2011期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2012期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2013期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2014期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2015期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2016期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2017期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2018期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2019期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2020期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2021期间收益,相对沪深300超额收益,相对创业板超额收益')

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
        
        #2011    
        if bgDate<='20110101' and edDate>='20111231':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20110101', '20111231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2011)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2011)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2012    
        if bgDate<='20120101' and edDate>='20121231':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20120101', '20121231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2012)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2012)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2013    
        if bgDate<='20130101' and edDate>='20131231':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20130101', '20131231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2013)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2013)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2014    
        if bgDate<='20140101' and edDate>='20141231':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20140101', '20141231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2014)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2014)*100,2)),end=',')
        else:
            print('-',end=',')  
            print('-',end=',')
            print('-',end=',')
        
        #2015    
        if bgDate<='20150101' and edDate>='20151231':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20150101', '20151231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2015)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2015)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2016    
        if bgDate<='20160101' and edDate>='20161231':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20160101', '20161231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2016)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2016)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
                
        #2017    
        if bgDate<='20170101' and edDate>='20171231':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20170101', '20171231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2017)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2017)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
                
        #2018       
        if bgDate<='20180101' and edDate>='20181231':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20180101', '20181231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2018)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2018)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2019    
        if bgDate<='20190101' and edDate>='20191231':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20190101', '20191231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2019)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2019)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
                
        #2020    
        if bgDate<='20200101' and edDate>='20201231':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20200101', '20201231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2020)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2020)*100,2)),end=',')
        else:
            print('-',end=',')       
            print('-',end=',')
            print('-',end=',')

        #2020    
        if bgDate<='20210101' and edDate>=end_dealday:
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20210101', '20211231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2021)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2021)*100,2)),end=',')
        else:
            print('-',end=',')       
            print('-',end=',')
            print('-',end=',')
    
    
        print()
    
        log.logger.info('%d：处理完%s基金经理最近十年在%s基金的数据'%(ix,mgrName,tsCode))    
    
        ix=ix+1
                    
    sys.stdout = savedStdout #恢复标准输出流
    
    #分析三：
    #对所有的基金，分析其最近十年的业绩表现
     
    sqlStr="SELECT\
    list.ts_code,\
    list.NAME fundName,\
    list.management,\
    list.issue_date issue_date\
    FROM\
    u_fund_list list\
    WHERE\
    list.market = 'O'\
    AND list.fund_type NOT LIKE '%货币%'\
    AND list.fund_type NOT LIKE '%债%'\
    AND list.fund_type NOT LIKE '%保本%'\
    AND list.fund_type NOT LIKE '%另类%'\
    AND list.fund_type NOT LIKE '%商品%'\
    AND list.issue_date <> ''\
    AND list.delist_date IS NULL\
    ORDER BY\
    list.ts_code"
    
    
    fdDF=fdMysqlProcessor.querySql(sqlStr)

    fdNo=len(fdDF)
              
    
    #分析所有基金经理在近10年内的表现情况    
    ix=0

    savedStdout=sys.stdout  #保存标准输出流
    sys.stdout=open('d:/fdPerfAna.csv','wt+',newline='',encoding='utf-8-sig')            
    
    print('序号,基金代码,基金名称,\
    2011期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2012期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2013期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2014期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2015期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2016期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2017期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2018期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2019期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2020期间收益,相对沪深300超额收益,相对创业板超额收益,\
    2021期间收益,相对沪深300超额收益,相对创业板超额收益')

    while(ix<fdNo):
        
        tsCode=fdDF.at[ix,'ts_code']
        fdName=fdDF.at[ix,'fundName']
        isDate=fdDF.at[ix,'issue_date']
        
        edDate=end_dealday

        print("%d,%s,%s"%(ix,tsCode,fdName),end=',')
        
        #2011    
        if isDate<='20110101':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20110101', '20111231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2011)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2011)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2012    
        if isDate<='20120101':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20120101', '20121231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2012)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2012)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2013    
        if isDate<='20130101':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20130101', '20131231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2013)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2013)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2014    
        if isDate<='20140101':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20140101', '20141231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2014)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2014)*100,2)),end=',')
        else:
            print('-',end=',')  
            print('-',end=',')
            print('-',end=',')
        
        #2015    
        if isDate<='20150101':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20150101', '20151231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2015)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2015)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2016    
        if isDate<='20160101':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20160101', '20161231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2016)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2016)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
         
        #2017    
        if isDate<='20170101':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20170101', '20171231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2017)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2017)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2018       
        if isDate<='20180101':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20180101', '20181231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2018)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2018)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2019    
        if isDate<='20190101':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20190101', '20191231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2019)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2019)*100,2)),end=',')
        else:
            print('-',end=',')
            print('-',end=',')
            print('-',end=',')
        
        #2020    
        if isDate<='20200101':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20200101', '20201231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2020)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2020)*100,2)),end=',')
        else:
            print('-',end=',')       
            print('-',end=',')
            print('-',end=',')
 
        #2021 
        if isDate<='20210101':
            incRate=anaFdPerfDuringPeriod(fdMysqlProcessor,tsCode, '20210101', '20211231')
            print('%f%%'%(round(incRate*100,2)),end=',')
            print('%f%%'%(round((incRate-hs300ir2021)*100,2)),end=',')
            print('%f%%'%(round((incRate-cybir2021)*100,2)),end=',')
        else:
            print('-',end=',')       
            print('-',end=',')
            print('-',end=',') 
    
        print()
    
        log.logger.info('%d：处理完%s基金最近十年的数据'%(ix,tsCode))    
    
        ix=ix+1
                    
    sys.stdout = savedStdout #恢复标准输出流   
'''
Created on 2020年6月28日

@author: xiaoda
'''
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import OUTPUTDIR
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockSelectStrategyUtil import StockSelectStrategyProcessor
from com.xiaoda.stock.loopbacktester.utils.TradeStrategyUtil import TradeStrategyProcessor
from pathlib import Path
from com.xiaoda.stock.loopbacktester.LoopbackTester import compAndOutputXLSAndPlot
import pandas as pd
import datetime
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
import sys
from pandas.core.frame import DataFrame

ResultOutDir='d:/resultdir/'


#对指定策略、指定日期，进行预处理
#即，如果已经有数据，直接取回
#如果还没有，则进行计算后，取回数据
def preProcessStrategyComp(stockSelectStrategyName,tradeSelectStrategyName,startdate,enddate):

    #enddate=sdf.at[0,'content']
    
    OODir=OUTPUTDIR+'/'+startdate+'-'+enddate
    irrSummaryLoc=OODir+'/'+stockSelectStrategyName+'/'+tradeSelectStrategyName+'/Summary-xirr.csv'
    myPath=Path(irrSummaryLoc)
    
    
    #如果该位置不存在
    #说明该区间的数据还未进行测算
    #需要进行测算
    #如果已经存在，说明已经有结果，可以直接取结果
    if not(myPath.exists()):
        compAndOutputXLSAndPlot(stockSelectStrategyName, tradeSelectStrategyName, startdate, enddate)

    irrDT=pd.read_csv(irrSummaryLoc,encoding='utf-8-sig')
    #剔除掉最后一行文字
    #显示所有列
    #pd.set_option('display.max_columns', None)
    #显示所有行
    #pd.set_option('display.max_rows',None) 
    
    if len(irrDT)>0:
        irrDT=irrDT.drop(irrDT.index[-1])
    #irrDT.set_index('日期',drop=True, inplace=True)
    else:
        irrDT=DataFrame()

    #print(irrDT)
    return irrDT







#计算对指定的策略，从指定时间开始
#每个月买入，分别需要多久获得正收益
def getTimePeriodToGetGoalRetForSingleStrategy(stockSelectStrategyName,tradeSelectStrategyName,\
                                                startdate,enddate,ddayStep,golRet):
    
    #timePeriodDict={}
    sdProcessor=StockDataProcessor()    
    
    #输出结果
    resultfile=ResultOutDir+'TimeToGetGoalRet-'+stockSelectStrategyName+'-'+startdate+'-'+enddate+'-'+str(golRet)+'-'+str(ddayStep)+'.csv' 
    
    savedStdout=sys.stdout  #保存标准输出流
    sys.stdout=open(resultfile,'wt+',newline='',encoding='utf-8-sig')
    print('初次买入日期',end=',')
    print('初次盈利'+str(golRet)+'的日期',end=',')
    print('间隔天数',end='\n') 
    sys.stdout = savedStdout #恢复标准输出流
 
    
    curdate=startdate
    #对从开始日期后的日期，进行循环计算多久开始产生正收益
    
    while(sdProcessor.getDateDistance(curdate, enddate)>0):
    
        #获取收益计算结果
        retXlsPD=preProcessStrategyComp(stockSelectStrategyName, tradeSelectStrategyName,curdate, enddate)
        
        #计算对于curdate购买，多久可以取得正收益
        #并写入到结果文件中
        
        #print(retXlsPD)
        
        #xlsPD=retXlsPD[['日期','持仓当日盈亏率']]
        
        daysToOverTH=-9999
        
        flg=False
        firstDateOverThreshHold=''
        for indexs in retXlsPD.index:
            #print(retXlsPD.loc[indexs].values[0:-1])
            if retXlsPD.loc[indexs]['当日收盘持仓总盈亏']/retXlsPD.loc[indexs]['截至当日资金净占用']>=golRet:
                flg=True
                firstDateOverThreshHold=retXlsPD.loc[indexs]['日期']
                
                #转换日期格式
                fdth=datetime.datetime.strptime(firstDateOverThreshHold,'%Y/%m/%d').strftime('%Y%m%d')
                
                break
        
        if flg==True:
            #说明收益率超过threshHold了
            daysToOverTH=sdProcessor.getDateDistance(curdate, fdth)
        else:
            #说明收益率始终没有超过threshHold
            pass
        
        #timePeriodDict[curdate]=daysToOverTH
        
        savedStdout=sys.stdout  #保存标准输出流
        sys.stdout=open(resultfile,'a+',newline='',encoding='utf-8-sig')        
        print(curdate,end=',')
        print(fdth,end=',')
        print(daysToOverTH,end='\n')
        sys.stdout = savedStdout #恢复标准输出流
        
        curdate=sdProcessor.getDealDayByOffset(curdate,ddayStep)
        
        if curdate==None:
            #已经超出日期范围
            break
        #curdate=sdProcessor.getNextDealDay(curdate, False)
    
    
#计算任意交易日买入，一年后获得的收益情况
def getRetRateAfterSomeTimeForSingleStrategy(stockSelectStrategyName,tradeSelectStrategyName,startdate,enddate,ddayStep,dDays):
    #timePeriodDict={}
    sdProcessor=StockDataProcessor()    
    
    #输出结果
    resultfile1=ResultOutDir+'RetRateAfterDDays-'+stockSelectStrategyName+'-'+startdate+'-'+enddate+'-'+str(ddayStep)+'-'+str(dDays)+'.csv' 
    
    
    #resultfile1保存dDays天后的收益情况
    savedStdout=sys.stdout  #保存标准输出流
    sys.stdout=open(resultfile1,'wt+',newline='',encoding='utf-8-sig')
    print('初次买入日期',end=',')
    print(str(dDays)+'天后的日期',end=',')
    print('投入资金量',end=',')
    print(str(dDays)+'天后的收益',end=',')
    print(str(dDays)+'天后的收益率',end='\n') 
    sys.stdout = savedStdout #恢复标准输出流
    
    #记录从Startdate开始，每一天的投入资金总额，以及回报总额
    profitEveryDayAfterStartdate={}
 
    
    curdate=startdate
    #对从开始日期后的日期，进行循环计算多久开始产生正收益
    

    
    while(sdProcessor.getDateDistance(curdate, enddate)>0):
    

        #获取收益计算结果
        retXlsPD=preProcessStrategyComp(stockSelectStrategyName, tradeSelectStrategyName,curdate, enddate)
        
        
        profitRateToNow=-999
        profitToNow=0
        totalInventToNow=0
        
        dDaysAfter=sdProcessor.getCalDayByOffset(curdate, dDays)
        
        realDate=''

        afterDDays=False
        for indexs in retXlsPD.index:
            #print(retXlsPD.loc[indexs].values[0:-1])
            
            
            dateAfterTran=datetime.datetime.strptime(retXlsPD.loc[indexs]['日期'],'%Y/%m/%d').strftime('%Y%m%d')
            totalInventToNow=retXlsPD.loc[indexs]['截至当日资金净占用']                
            profitToNow=retXlsPD.loc[indexs]['当日收盘持仓总盈亏']
                        
            if dateAfterTran in profitEveryDayAfterStartdate:
                totalInventBefore=profitEveryDayAfterStartdate[dateAfterTran][0]
                totalProfitBefore=profitEveryDayAfterStartdate[dateAfterTran][1]
                
                
                profitEveryDayAfterStartdate[dateAfterTran]=[totalInventBefore+totalInventToNow,totalProfitBefore+profitToNow]
            else:

                profitEveryDayAfterStartdate[dateAfterTran]=[totalInventToNow,profitToNow]         
            
            
            #找到dDays之后的那一天
            if afterDDays==False and dateAfterTran>=dDaysAfter:
                
                profitRateToNow=profitToNow/totalInventToNow
                realDate=dateAfterTran
                
                #输出dDays天后的盈利状况
                savedStdout=sys.stdout  #保存标准输出流
                sys.stdout=open(resultfile1,'a+',newline='',encoding='utf-8-sig')        
                print(curdate,end=',')
                print(realDate,end=',')
                print(totalInventToNow,end=',')
                print(profitToNow,end=',')
                print(profitRateToNow,end='\n')
                sys.stdout = savedStdout #恢复标准输出流
                afterDDays=True
        

        
        curdate=sdProcessor.getDealDayByOffset(curdate,ddayStep)
        
        if curdate==None:
            #已经超出日期范围
            break
    
        savedStdout=sys.stdout  #保存标准输出流
    
    
    
    
    #输出结果
    resultfile2=ResultOutDir+'RetRateEveryDayAfter-'+stockSelectStrategyName+'-'+startdate+'-'+enddate+'-'+str(ddayStep)+'-'+str(dDays)+'.csv' 
        
    sys.stdout=open(resultfile2,'wt+',newline='',encoding='utf-8-sig')
    print('日期',end=',')
    print('投入资金总量',end=',')
    print('当天累计总收益',end='\n')

    for k,v in profitEveryDayAfterStartdate.items():
        
        print(k,end=',')
        print(v[0],end=',')
        print(v[1],end='\n')
    
    sys.stdout = savedStdout #恢复标准输出流
    #pass
    
    


#计算对指定的策略，在指定时间段内获得的收益率
def getProfitRateForSingleStrategy(stockSelectStrategyName,tradeSelectStrategyName,startdate,enddate):
    #stockSelectStrategy=StockSelectStrategyProcessor().getStrategy(stockSelectStrategyName)
    #tradeStrategy=TradeStrategyProcessor().getStrategy(tradeSelectStrategyName)    

    #计算获得收益率变化数据
    data=preProcessStrategyComp(stockSelectStrategyName,tradeSelectStrategyName,startdate,enddate)
    
    #print(data)
    
    avgProfitRate=0    
    return avgProfitRate
    
    
    
if __name__ == '__main__':
    '''
    穷举各个时间，计算每一个买入日期之下，获得正收益需要的时间，以及获得20%，30%，50%收益需要的时间
    半年内获得正收益的概率，一年之内获得正收益的概率，一年半、两年获得正收益的概率
    买入后，一年、两年、三年内平均年化收益
  买入后，超越指数的概率，平均超越指数的数量
  
    '''
    
    #起始测试日期
    #可以每月测一次，看看以任何一个月初进行计算
    startdate='20180101'
    #startdate='20180123'
    sdProcessor=StockDataProcessor()  
    startdate=sdProcessor.getNextDealDay(startdate, True)

    #结束测试日期,默认应该是当前日期
    #curr_time=datetime.datetime.now()
    #enddate=curr_time.strftime("%Y%m%d")
    #指定结束日期
    enddate='20201202'
    
    mysqlProcessor=MysqlProcessor()
    sdf=mysqlProcessor.querySql('select content from u_data_desc where content_name=\'data_end_dealday\'')
    if enddate>sdf.at[0,'content']:
        enddate=sdf.at[0,'content']
    
    
    #用于对照的指数
    #cmpIdx='HS300'
    
    #stockSelectStrategyName='ROEStrategy'
    #stockSelectStrategyName='ROICStrategy'
    #stockSelectStrategyName='CCPlusNPRStrategy'
    stockSelectStrategyName='CashCowStrategy'
    tradeSelectStrategyName='HoldStrategy'
    
    
    #回测间隔交易步日数量
    ddayStep=90
    #目标收益率
    goalRet=0.2
    
    #1、计算获取到某一目标收益，分别需要用多少天时间
    #getTimePeriodToGetGoalRetForSingleStrategy(stockSelectStrategyName,tradeSelectStrategyName,startdate,enddate,ddayStep,goalRet)
    
    #测算时间周期
    dDays=365
    
    #2、计算任意交易日买入，一段时间后获得的收益情况
    getRetRateAfterSomeTimeForSingleStrategy(stockSelectStrategyName,tradeSelectStrategyName,startdate,enddate,ddayStep,dDays)
    
    
    
    
    
    
    pass
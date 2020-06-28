'''
Created on 2019年10月18日

@author: picc
'''
#import tushare
#import math
import sys
from pathlib import Path
import os
from com.xiaoda.stock.loopbacktester.utils.TradeChargeUtils import TradeChargeProcessor
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import OUTPUTDIR
import datetime
from datetime import datetime as dt
#import shutil
from sqlalchemy.util.langhelpers import NoneType
from com.xiaoda.stock.loopbacktester.utils.FileUtils import FileProcessor
from com.xiaoda.stock.loopbacktester.utils.IRRUtils import IRRProcessor
from timeit import default_timer as timer
import time
import pandas
import math

from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor

from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger

import multiprocessing

from multiprocessing import Manager
from com.xiaoda.stock.loopbacktester.utils.TradeStrategyUtil import TradeStrategyProcessor
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockSelectStrategyUtil import StockSelectStrategyProcessor

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA


'''
def fn_timer(fn):
    def function_timer(*args, **kwargs):
        """装饰器"""
        from time import time
        t = time()
        result = fn(*args, **kwargs)
        print('【%s】运行%.4f秒' % (fn.__name__, time() - t))
        return result
    return function_timer

'''

#@fn_timer

log=Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')

tradeStrategyProcessor=TradeStrategyProcessor()


''
def printStockOutputHead(outputFile):
    #origStdout=sys.stdout  #保存标准输出流
    #sys.stdout=open(outputFile,'wt')
    print('日期,交易类型,当天收盘持仓市值,当天持仓总手数,累计投入,累计赎回,当天资金净占用,当天资金净流量,当前持仓平均成本,\
    当天收盘价格,当前持仓盈亏,最近一次交易类型,最近一次交易价格,当前全部投入回报率,本次交易手续费,当天是否交易(可能非交易日或停牌)')
    #sys.stdout=origStdout  #恢复标准输出流


#@fn_timer  
def printTradeInfo(outputFile,currday,dealType,closePriceToday,holdShares,holdAvgPrice,netCashFlowToday,
                   totalInput,totalOutput,latestDealType,latestDealPrice,dealCharge):
    #origStdout=sys.stdout  #保存标准输出流
    #sys.stdout=open(outputFile,'at+')
    
    currentProfit=round(closePriceToday*holdShares*100+totalOutput-totalInput,4)
    if totalInput>0:
        totalProfitRate=currentProfit/totalInput*100
    else:
        totalProfitRate=0
        
    returnDF=pandas.DataFrame([[currday,\
                                dealType,\
                                round(closePriceToday*holdShares*100,4),\
                                holdShares,\
                                round(totalInput,4),\
                                round(totalOutput,4),\
                                round(totalInput-totalOutput,4),\
                                round(netCashFlowToday,4),\
                                round(holdAvgPrice,4),\
                                round(closePriceToday,4),\
                                currentProfit,\
                                latestDealType,\
                                round(latestDealPrice,4),\
                                "%.2f%%"%(totalProfitRate),\
                                round(dealCharge,2),\
                                1]])

    valStr=returnDF[0:1].to_csv(index=False,header=False,sep=",")
    valStr=valStr.strip()
    print(valStr)
    #sys.stdout=origStdout  #恢复标准输出流
    return returnDF

#@fn_timer
#对于非交易日，需要将上一交易日数据重新输出一行
def dupLastLineinFile(currday,outputFile,lastDealDayDF):
    
    #ticdup=timer()
    
    #origStdout=sys.stdout  #保存标准输出流
    #sys.stdout=open(outputFile,'at+')
    #修改当天日期
    lastDealDayDF.iat[0,0]=currday
    #交易类型
    lastDealDayDF.iat[0,1]=0
    #当天资金净流量
    lastDealDayDF.iat[0,7]=0
    #当天交易手续费
    lastDealDayDF.iat[0,14]=0
    #当天不开始交易：非交易日或停牌
    lastDealDayDF.iat[0,15]=0
    
    valStr=lastDealDayDF[0:1].to_csv(index=False,header=False,sep=",")
    valStr=valStr.strip()
    print(valStr)
    #sys.stdout=origStdout  #恢复标准输出流
    #tocdup=timer()
    #log.logger.info("dup部分代码耗时:%s"%(tocdup-ticdup))
   
'''
#@fn_timer
def printSummaryOutputHead(outputFile):
    origStdout=sys.stdout  #保存标准输出流
    sys.stdout=open(outputFile,'wt')
    print('股票代码,最大资金占用,累计资金投入,累计资金赎回,最新盈亏,当前持仓金额')
    sys.stdout=origStdout  #恢复标准输出流

#@fn_timer
def printSummaryTradeInfo(outputFile,stockCode, biggestCashOccupy, totalInput,totalOutput,latestProfit,holdShares,closePriceToday):
    origStdout=sys.stdout  #保存标准输出流
    sys.stdout=open(outputFile,'at+')
    print('\''+str(stockCode),end=', ')
    print(round(biggestCashOccupy,2), end=', ')
    print(round(totalInput,2), end=', ')
    print(round(totalOutput,2), end=', ')    
    print(latestProfit, end=', ')
    print(round(holdShares*100*closePriceToday,2), end='\n')
    sys.stdout=origStdout  #恢复标准输出流
'''

def get8slListFromStockList(stockList):
    retSLList=[]
    slLen=len(stockList)
    #对所有股票进行遍历
    i=0
    tmpList1=[]
    tmpList2=[]
    tmpList3=[]
    tmpList4=[]
    tmpList5=[]
    tmpList6=[]
    tmpList7=[]
    tmpList8=[]
    
    for stockCode in stockList:
        #log.logger.info(stockCode)
        if i<slLen*1/8:
            tmpList1.append(stockCode)
        elif i>=slLen*1/8 and i<slLen*2/8:
            tmpList2.append(stockCode)
        elif i>=slLen*2/8 and i<slLen*3/8:
            tmpList3.append(stockCode)
        elif i>=slLen*3/8 and i<slLen*4/8:
            tmpList4.append(stockCode)
        elif i>=slLen*4/8 and i<slLen*5/8:
            tmpList5.append(stockCode)
        elif i>=slLen*5/8 and i<slLen*6/8:
            tmpList6.append(stockCode)
        elif i>=slLen*6/8 and i<slLen*7/8:
            tmpList7.append(stockCode)
        else:
            tmpList8.append(stockCode)            
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

#具体处理股票的处理
#@fn_timer
def processStock(stockList,tradeStrategyName,strOutputDir,firstDealDay,enddate):
    
    #t1=timer()
    #print("t1:%s"%(t1))
    tradeStrategy=tradeStrategyProcessor.getStrategy(tradeStrategyName)
    '''        
    lastDealDay=sdProcessor.getLastDealDay(enddate,True)
    '''
    
    #t2=timer()
    #print("t2:%s"%(t2))
    #print("t2-t1:%s"%(t2-t1))
    #对股票列表中的每一个列表进行计算
    for stockCode in stockList:
        
        #t3=timer()
        #print("t3:%s"%(t3))
        outputFile = strOutputDir+'/'+ stockCode + '.csv'
        myPath=Path(outputFile)
    
        #如果文件已经存在，说明已经处理过了，直接跳过该股票即可
        if myPath.exists():
            log.logger.info('%s已处理过'%(stockCode))
            continue
        #t4=timer()
        #print("t4:%s"%(t4))
        stockTradeDF=tradeStrategy.processTradeDuringPeriod(stockCode,firstDealDay,enddate)
        
        #t5=timer()
        #print("t5:%s"%(t5))
        if stockTradeDF.empty:
            continue
        else:
            stockTradeDF.to_csv(outputFile,index=False,encoding='ANSI')
        #t6=timer()
        #print("t6:%s"%(t6))
        #print("t6-t3:%s"%(t6-t3))
        #print("t6-t4:%s"%(t6-t4))
        #print("t6-t5:%s"%(t6-t5))
        



#进行计算，输出Excel，并画图   
def compAndOutputXLSAndPlot(stockSelectStrategyString,tradeStrategyString,startdate,enddate):
    
    stockSelectStrategyList=stockSelectStrategyString.split(',')
    tradeStrategyList=tradeStrategyString.split(',')

    sdProcessor=StockDataProcessor()
    
    #通过STARTDATE找到第一个交易日
    firstDealDay=sdProcessor.getNextDealDay(startdate,True)
    
    #需要找到开始日期前面的20个交易日那天，从那一天开始获取数据
    #可能有企业临时停牌的问题，向前找20个交易日，有可能不够在后面扣除
    #向前找30个交易日

    twentyDaysBeforeFirstOpenDay=sdProcessor.getDealDayByOffset(firstDealDay, -30)
    
    OODir=OUTPUTDIR+'/'+startdate+'-'+enddate
    
    
    if not(Path(OODir).exists()):
        os.mkdir(OODir)
    
    
    for stockSelectStrategyStr in stockSelectStrategyList:
        
        stockSelectStrategy=StockSelectStrategyProcessor().getStrategy(stockSelectStrategyStr)

        if stockSelectStrategy==None:
            log.logger.error('StockSelectStrategy：%s输入错误'%(stockSelectStrategyStr))
            continue
        
        log.logger.info('%s,开始处理选股策略:%s'%(os.getpid(),stockSelectStrategy.getStrategyName()))
        #从参数获取股票选取策略
        stockList=stockSelectStrategy.getSelectedStockList(sdProcessor,startdate)
        
        strOutterOutputDir=OODir+'/'+stockSelectStrategy.getStrategyName()
        
        myPath=Path(strOutterOutputDir)
        
        #如果该位置存在，则直接使用该位置，不用删除掉重新算
        if not(myPath.exists()):
            os.mkdir(strOutterOutputDir)
        #从参数获取交易策略
  
        
        #对所有策略进行循环：
        for tradeStrategyStr in tradeStrategyList:
            
            tradeStrategy=TradeStrategyProcessor().getStrategy(tradeStrategyStr)

            if tradeStrategy==None:
                log.logger.error('TradeStrategy：%s输入错误'%(tradeStrategyStr))
                continue
            
            
            log.logger.info('%s,开始处理交易策略:%s'%(os.getpid(),tradeStrategy.getStrategyName()))
            #savedStdout = sys.stdout  #保存标准输出流
             
            strOutputDir=strOutterOutputDir+'/'+tradeStrategy.getStrategyName()
            
            myPath=Path(strOutputDir)
        
            #如果该位置存在，则直接使用该位置，不用删除掉重新算
            if not(myPath.exists()):
                os.mkdir(strOutputDir)
            
            summaryOutFile=strOutputDir+'/Summary.csv'
            
            '''
            #myPath=Path(summaryOutFile)
            #如果Summary.csv已经存在，则直接追加即可，不用往里面继续写入抬头
            if not(myPath.exists()):
                #追加方式写入，针对如果已经处理了一半的策略
                #sys.stdout = open(outputFile,'at+')
                printSummaryOutputHead(summaryOutFile)
            #sys.stdout = savedStdout  #恢复标准输出流
            '''
            #对所有股票代码，循环进行处理
            #in_text = open(INPUTFILE, 'r')
            #直接对stockList进行遍历，不需要通过INPUTFILE获取股票列表
            
            #循环所有股票，使用指定策略进行处理
        #    for line in in_text.readlines():  
        #        stockCode = line.rstrip("\n")
            #latestholdAmtDict={}
            
            #把股票代码分成8个list
            slList=get8slListFromStockList(stockList)
            
            manager = Manager()
            
            #分8个进程，分别计算8段股票波动率
            
            process=[]
        
        
            for subStockList in slList:
                
                p=multiprocessing.Process(target=processStock,args=(subStockList,tradeStrategy.getStrategyName(),strOutputDir,\
                                 firstDealDay,enddate,))
                p.start()
        
                process.append(p)
            
            for p in process:
                p.join()

            #完成所有股票的数据处理
            #需要通过扫描所有股票的计算结果
            #计算出整体的IRR

            log.logger.info('开始处理IRR及Summary计算')
            #读取结果文件列表
            stockfileList=os.listdir(strOutputDir)
            
            #记录csv内容的列表
            fileContentTupleList=[]
            
            #对文件列表中的文件进行处理，获取内容列表
            for stockfileStr in stockfileList:
                if not 'Summary' in stockfileStr:
                    df=FileProcessor.readFile(strOutputDir+'/'+stockfileStr)
                    fileContentTupleList.append((stockfileStr[:-4],df))

            #log.logger.info("开始处理Summary计算")


            savedStdout=sys.stdout  #保存标准输出流
            sys.stdout=open(strOutputDir+'/Summary.csv','wt+')            
            
            print('股票代码,股票名称,所属行业,最大资金占用,累计资金投入,累计资金赎回,最新盈亏,当前持仓金额')
            
            #对已有的内容列表进行处理
            for stockCode,stockfileDF in fileContentTupleList:
                
                stInfoDF=sdProcessor.getStockInfo(stockCode)
                stName=stInfoDF.at[0,'name']
                stInd=stInfoDF.at[0,'industry']
                
                biggestCashOccupy=0
                totalIn=0
                totalOut=0
                latestProfit=0
                currMarketValue=0
                idx=0
                while True:

                    if float(stockfileDF.at[idx,'当天资金净占用'])>biggestCashOccupy:
                        biggestCashOccupy=float(stockfileDF.at[idx,'当天资金净占用'])
                    
                    idx=idx+1
                    if idx==len(stockfileDF):
                        totalIn=stockfileDF.at[idx-1,'累计投入金额']
                        totalOut=stockfileDF.at[idx-1,'累计赎回金额']
                        latestProfit=stockfileDF.at[idx-1,'当前持仓盈亏']
                        currMarketValue=stockfileDF.at[idx-1,'当天收盘持仓市值']
                        break
                
                print(stockCode,end=',')
                print(stName,end=',')
                print(stInd,end=',')
                print(biggestCashOccupy,end=',')            
                print(totalIn,end=',')
                print(totalOut,end=',')
                print(latestProfit,end=',')
                print(currMarketValue)
                                            
            sys.stdout = savedStdout #恢复标准输出流
            
            
            '''
            #@fn_timer
            def printSummaryOutputHead(outputFile):
                origStdout=sys.stdout  #保存标准输出流
                sys.stdout=open(outputFile,'wt')
                print('股票代码,最大资金占用,累计资金投入,累计资金赎回,最新盈亏,当前持仓金额')
                sys.stdout=origStdout  #恢复标准输出流
            
            #@fn_timer
            def printSummaryTradeInfo(outputFile,stockCode, biggestCashOccupy, totalInput,totalOutput,latestProfit,holdShares,closePriceToday):
                origStdout=sys.stdout  #保存标准输出流
                sys.stdout=open(outputFile,'at+')
                print('\''+str(stockCode),end=', ')
                print(round(biggestCashOccupy,2), end=', ')
                print(round(totalInput,2), end=', ')
                print(round(totalOutput,2), end=', ')    
                print(latestProfit, end=', ')
                print(round(holdShares*100*closePriceToday,2), end='\n')
                sys.stdout=origStdout  #恢复标准输出流
            '''            
            
            
            

        
        
        
            #对各个日期计算相应的资金净流量
            cashFlowDict= {}
            #对已有的内容列表进行处理
            for stockfileName,stockfileDF in fileContentTupleList:
                #print(stockfileName)
            
                #如果Summary-all.csv已经存在，则直接覆盖
              
                idx=0
                while True:
                    #if stockfileDF.at[idx,'当天资金净流量']==None:
                    #    print()
                    if not (stockfileDF.at[idx,'日期'] in cashFlowDict):
                        cashFlowDict[stockfileDF.at[idx,'日期']]=\
                        float(stockfileDF.at[idx,'当天资金净流量']),\
                        float(stockfileDF.at[idx,'当天资金净占用']),\
                        float(stockfileDF.at[idx,'当前持仓盈亏']),\
                        float(stockfileDF.at[idx,'当天收盘持仓市值'])
                    else:    
                        cashFlowDict[stockfileDF.at[idx,'日期']]=\
                        float(cashFlowDict[stockfileDF.at[idx,'日期']][0])+float(stockfileDF.at[idx,'当天资金净流量']),\
                        float(cashFlowDict[stockfileDF.at[idx,'日期']][1])+float(stockfileDF.at[idx,'当天资金净占用']),\
                        float(cashFlowDict[stockfileDF.at[idx,'日期']][2])+float(stockfileDF.at[idx,'当前持仓盈亏']),\
                        float(cashFlowDict[stockfileDF.at[idx,'日期']][3])+float(stockfileDF.at[idx,'当天收盘持仓市值'])
                        #if float(stockfileDF.at[i,'当天收盘持仓市值'])%1>0:
                        #    print()
                    idx=idx+1
                    if idx==len(stockfileDF):
                        #最后一天，要把当天的持仓增加到净现金流
                        #以便计算XIRR
                        cashFlowDict[stockfileDF.at[idx-1,'日期']]=\
                        (float(cashFlowDict[stockfileDF.at[idx-1,'日期']][0])+float(stockfileDF.at[idx-1,'当天收盘持仓市值'])),\
                        cashFlowDict[stockfileDF.at[idx-1,'日期']][1],\
                        cashFlowDict[stockfileDF.at[idx-1,'日期']][2],\
                        cashFlowDict[stockfileDF.at[idx-1,'日期']][3]
                        
                        #log.logger.info("IRR Summary:%s股票的最后一天持仓市值:%f"%(stockfileName,float(stockfileDF.at[idx-1,'当天收盘持仓市值'])))
                        #由于存在股票到实际endday之前就已经停牌的情况，在股票的数据里面没有数据，所以按天进行处理会导致无数据的情况
                        #导致了summary-irr和summary的持仓数据不同的情况
                        #summary-irr按照日期进行统计，导致了已经停牌的股票，在停牌后到endday，都没有当日持仓金额
                        #要彻底解决这个问题，不是在这里处理，而应当到处理股票数据的逻辑中进行处理
                        #不论股票在某个交易日是否停牌，只要是交易日，都需要在输出时有一行输出
                        #if (float(stockfileDF.at[idx-1,'当天收盘持仓市值']))>0:
                        #    print("%s:%s:%s:%s"%(stockfileName,float(stockfileDF.at[idx-1,'当天收盘持仓市值']),cashFlowDict[stockfileDF.at[idx-1,'日期']][3],cashFlowDict[stockfileDF.at[idx-1,'日期']][3]))
                        break
            
            #对字典进行一下排序
            sorted(cashFlowDict)
            
            savedStdout=sys.stdout  #保存标准输出流
            sys.stdout=open(strOutputDir+'/Summary-xirr.csv','wt+')
            
            cashFlowList=[]
            print('日期,当日发生资金净流量,截至当日资金净占用,当日收盘持仓总盈亏,当日收盘持仓总市值,持仓当日产生盈亏,持仓当日盈亏率,创业板指数收盘,沪深300指数收盘')
            keysList=list(cashFlowDict.keys())
            keysList.sort()
            yesterdayTotalProfit=0


            lastIdxClose=0
            
            
            pltDF=pandas.DataFrame(data=[],columns=['Date','ProIncRate','CYBIncRate','HS300'])
            
            #获取最初的资金投入，以作为后续计算的依据
            #不能用这个，因为可能会有某些股票在初始停牌，
            #后续才真正可交易，产生了资金投入
            #origInput=math.fabs(cashFlowDict.get(keysList[0])[0])
            
            dtStr=(keysList[0])[0:8]
            
            #idxDF=sdProcessor.getidxData('HS300',dtStr,dtStr)
            
            idxDF1=sdProcessor.getidxData('CYB',dtStr,dtStr)
            origIdxClose1=idxDF1.at[dtStr,'close']

            idxDF2=sdProcessor.getidxData('HS300',dtStr,dtStr)
            origIdxClose2=idxDF2.at[dtStr,'close']
            
            
            for key in keysList:
                
                dtStr=key[0:8]
                
                
                #如果不是交易日
                #直接取上一交易日收盘点数
                if sdProcessor.isDealDay(dtStr):
                    #根据日期，获取当天收盘指数
                    #idxDF=sdProcessor.getidxData('HS300',dtStr,dtStr)
                    idxDF1=sdProcessor.getidxData('CYB',dtStr,dtStr)
                    idxClose1=idxDF1.at[dtStr,'close']
                    lastIdxClose1=idxClose1
                    
                    idxDF2=sdProcessor.getidxData('HS300',dtStr,dtStr)
                    idxClose2=idxDF2.at[dtStr,'close']
                    lastIdxClose2=idxClose2
                else:
                    idxClose1=lastIdxClose1
                    idxClose2=lastIdxClose2
                
                #日期
                print(key[0:4]+'/'+key[4:6]+'/'+key[6:8],end=',')
                #当日发生资金净流量
                print(cashFlowDict.get(key)[0],end=',')
                totalCashOccupy=cashFlowDict.get(key)[1]
                #截至当日资金净占用
                print(cashFlowDict.get(key)[1],end=',')
                #当前持仓总盈亏
                print(cashFlowDict.get(key)[2],end=',')
                todayTotalProfit=cashFlowDict.get(key)[2]
                #当天收盘持仓总市值
                print(cashFlowDict.get(key)[3],end=',')
                #持仓当日产生盈亏
                todayProfit=todayTotalProfit-yesterdayTotalProfit
                print(todayProfit,end=',')
                yesterdayTotalProfit=todayTotalProfit
                
                #持仓当日盈亏率
                if float(cashFlowDict.get(key)[3])==0:
                    print(0,end=',')
                else:
                    print(round(todayProfit/(float(cashFlowDict.get(key)[3])-todayProfit),4),end=',')
                
                #log.logger.info("在%s日期的利润率:%.2f"%(dtStr,round(todayTotalProfit/origInput,4)))
                #创业板指数
                print(idxClose1,end=',')
                print(idxClose2)
                
                try:
                    tmpDF=pandas.DataFrame({'Date':dtStr,'ProIncRate':round(todayTotalProfit/totalCashOccupy,4),\
                                            'CYBIncRate':round(idxClose1/origIdxClose1-1,4),\
                                            'HS300IncRate':round(idxClose2/origIdxClose2-1,4)},index=[1])
                except:
                    print()

                pltDF=pltDF.append(tmpDF,ignore_index=True,sort=False)
    
                
                cashFlowList.append((datetime.date(int(key[0:4]),int(key[4:6]),int(key[6:8])),float(cashFlowDict.get(key)[0])))
            
            print(tradeStrategy.getStrategyName()+'在%s到%s期间内IRR为：'%(startdate,enddate),end=',')
            print(IRRProcessor.xirr2(cashFlowList))
            
            sys.stdout = savedStdout #恢复标准输出流
            
            
            
            

            plt.plot([dt.strptime(d,'%Y%m%d').date() for d in pltDF['Date'].to_list()],\
                     pltDF['ProIncRate'].to_list(),label='ProfitRate',c='blue')
            plt.plot([dt.strptime(d,'%Y%m%d').date() for d in pltDF['Date'].to_list()],\
                     pltDF['CYBIncRate'].to_list(),label='CYBRate',c='green')
            plt.plot([dt.strptime(d,'%Y%m%d').date() for d in pltDF['Date'].to_list()],\
                     pltDF['HS300IncRate'].to_list(),label='HS300',c='red')
            
            
            plt.title('Return ratio:%s,%s'%(stockSelectStrategyStr,tradeStrategyStr),fontsize=10)
            #设置图表标题和标题字号
            
            plt.tick_params(axis='both',which='major',labelsize=8)
            #设置刻度的字号
            
            plt.xlabel('Date',fontsize=8)
            #设置x轴标签及其字号
            
            plt.ylabel('IncRate',fontsize=8)
            #设置y轴标签及其字号
           
            #pltDF.plot()
            plt.legend()#显示图例，如果注释改行，即使设置了图例仍然不显示
            plt.grid(True)
            
            plt.savefig(strOutputDir+'/Summary.png')
            plt.savefig(OODir+'/'+stockSelectStrategyStr+'-'+tradeStrategyStr+'-'+'Summary.png')
            
            plt.cla()
            plt.clf()
            plt.close()
            
            '''
            from matplotlib.pyplot import MultipleLocator
            #从pyplot导入MultipleLocator类，这个类用于设置刻度间隔

            x_major_locator=MultipleLocator(1)
            #把x轴的刻度间隔设置为1，并存在变量里
            y_major_locator=MultipleLocator(10)
            #把y轴的刻度间隔设置为10，并存在变量里
            ax=plt.gca()
            #ax为两条坐标轴的实例
            ax.xaxis.set_major_locator(x_major_locator)
            #把x轴的主刻度设置为1的倍数
            ax.yaxis.set_major_locator(y_major_locator)
            #把y轴的主刻度设置为10的倍数
            plt.xlim(-0.5,11)
            #把x轴的刻度范围设置为-0.5到11，因为0.5不满一个刻度间隔，所以数字不会显示出来，但是能看到一点空白
            plt.ylim(-5,110)
            #把y轴的刻度范围设置为-5到110，同理，-5不会标出来，但是能看到一点空白
            '''

            
            '''
            host=host_subplot(111, axes_class=AA.Axes)
            plt.subplots_adjust(right=0.75)
            
            par1=host.twinx()
            #par2=host.twinx()
            
            #offset = 100
            #new_fixed_axis = par2.get_grid_helper().new_fixed_axis
            #par2.axis["right"]=new_fixed_axis(loc="right",axes=par2,offset=(offset, 0))
            
            par1.axis["right"].toggle(all=True)
            #par2.axis["right"].toggle(all=True)
            
            #host.set_xlim(0, 2)
            #host.set_ylim(0, 2)
            
            host.set_xlabel("Date")
            
            host.set_ylabel("Profit")
            par1.set_ylabel("HS300")
            #par2.set_ylabel("Velocity")
            
            p1,=host.plot(pltDF['Date'].to_list(),pltDF['Profit'].to_list(), label="Profit")
            p2,=par1.plot(pltDF['Date'].to_list(),pltDF['HS300'].to_list(), label="HS300")
            #p3, = par2.plot([0, 1, 2], [50, 30, 15], label="Velocity")
            
            par1.set_ylim(0, 4)
            #par2.set_ylim(1, 65)
            
            host.legend()
            
            host.axis["left"].label.set_color(p1.get_color())
            par1.axis["right"].label.set_color(p2.get_color())
            #par2.axis["right"].label.set_color(p3.get_color())
            
            plt.savefig(strOutputDir+'/Summary.png')
            '''
        
import argparse
import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
    
    # 创建命令行解析器句柄，并自定义描述信息
    parser = argparse.ArgumentParser(description="input all parameters")
    # 定义必选参数 positionArg
    # parser.add_argument("project_name")
    # 定义可选参数module
    parser.add_argument("--stockstrategy","-ss",type=str, default="RawStrategy",help="Enter the stock select strategy")
    parser.add_argument("--tradestrategy", "-ts",type=str, default="SimpleStrategy",help="Enter the trade strategy")
    parser.add_argument("--startdate", "-sd",type=str, default="20190701",help="Enter the start date")
    parser.add_argument("--enddate", "-ed",type=str, default="20191031",help="Enter the end date")
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

    stockSelectStrategyString=''
    tradeStrategyString=''
    startdate=''
    enddate=''
    
    for k, v in params.items():
        if k=='stockstrategy':
            stockSelectStrategyString=v
        elif k=='tradestrategy':
            tradeStrategyString=v
        elif k=='startdate':
            startdate=v
        elif k=='enddate':
            enddate=v    
    mysqlProcessor=MysqlProcessor()
    sdf=mysqlProcessor.querySql('select content from u_data_desc where content_name=\'data_end_dealday\'')
    if enddate>sdf.at[0,'content']:
        enddate=sdf.at[0,'content']
    
    if enddate<=startdate:
        log.logger.error('开始时间'+startdate+'晚于或等于结束时间'+enddate)
        pass
    else:
        compAndOutputXLSAndPlot(stockSelectStrategyString, tradeStrategyString, startdate, enddate)

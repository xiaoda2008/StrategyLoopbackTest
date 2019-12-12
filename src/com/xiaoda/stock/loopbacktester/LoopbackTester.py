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
from datetime import datetime as dt
import datetime
#import shutil
from sqlalchemy.util.langhelpers import NoneType
from com.xiaoda.stock.loopbacktester.utils.FileUtils import FileProcessor
from com.xiaoda.stock.loopbacktester.utils.IRRUtils import IRRProcessor
from timeit import default_timer as timer

import pandas

from com.xiaoda.stock.loopbacktester.strategy.trade.BuylowSellhighStrategy import BuylowSellhighStrategy
from com.xiaoda.stock.loopbacktester.strategy.trade.MultiStepStrategy import MultiStepStrategy
from com.xiaoda.stock.loopbacktester.strategy.trade.SMAStrategy import SMAStrategy

from com.xiaoda.stock.loopbacktester.strategy.stockselect.RawStrategy import RawStrategy
from com.xiaoda.stock.loopbacktester.strategy.stockselect.CashCowStrategy import CashCowStrategy
from com.xiaoda.stock.loopbacktester.strategy.stockselect.ROEStrategy import ROEStrategy
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor

from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger

import time
from functools import wraps
  
def fn_timer(fn):
    def function_timer(*args, **kwargs):
        """装饰器"""
        from time import time
        t = time()
        result = fn(*args, **kwargs)
        print('【%s】运行%.4f秒' % (fn.__name__, time() - t))
        return result
    return function_timer



#@fn_timer


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

log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    

#具体处理股票的处理
#@fn_timer
def processStock(sdProcessor,stockCode,strategy,strOutputDir,firstDealDay,lastDealDay,twentyDaysBeforeFirstDay,enddate,summaryOutFile):
    

    #stock_k_data = tushare.pro_bar(ts_code=stockCode,adj='qfq',
    #                               start_date=twentyDaysBeforeFirstDay,end_date=ENDDATE)

    stock_k_data=sdProcessor.getStockKData(stockCode,twentyDaysBeforeFirstDay,enddate,'qfq')

    #sprint(stock_k_data.columns)

    if type(stock_k_data)==NoneType or stock_k_data.empty:
        #如果没有任何返回值，说明该日期后没有上市交易过该股票
        log.logger.info('%s在%s（前推30个交易日）到%s区间内无交易，剔除'%(stockCode,twentyDaysBeforeFirstDay,enddate))
        return

#    stock_k_data = tushare.get_k_data(code=stockCode,start=twentyDaysBeforeFirstDay,end=ENDDATE)
    #stock_k_data.sort_index(inplace=True,ascending=True)

    stock_k_data.reset_index(drop=True,inplace=True)

    #错位一下，以便计算的MA20不包含当天的close数据
    #stock_k_data['close_shift']=stock_k_data['close'].shift(1)
    #直接用pre_close即可，不用计算

    #计算出MA20的数据，问题在于，这个MA20是包含当天的，有些问题，应当不包含当天
    stock_k_data['pre_MA20'] = stock_k_data['pre_close'].rolling(20).mean()
    
    #获得前一天的最高值和最低值
    stock_k_data['pre_high'] = stock_k_data['high'].shift(1)
    stock_k_data['pre_low'] = stock_k_data['low'].shift(1)
    
    
    #stock_k_data['today_MA20'] = stock_k_data['close'].rolling(20).mean()
    
    
    offset = stock_k_data.index[0]
    
    #剔除掉向前找的20个交易日数据
    #不要这样剔除，而是一直剔除到firstOpenDay

    a=0
    while True:
        
        #删空了，在向前推进的交易日有交易，但在查询区间内股票就没有交易
        if stock_k_data.empty:
            break
        
        #不允许正好这一天股票停牌
        if stock_k_data.at[a+offset,'trade_date']==firstDealDay:
            break
        elif stock_k_data.at[a+offset,'trade_date']>firstDealDay:
            #直接大于，说明第一天该股票就停牌，不进行处理
            return
        else:
            stock_k_data = stock_k_data.drop([offset+a])
            a=a+1

#    else:,offset+2,offset+3,offset+4,offset+5,offset+6,offset+7,offset+8,offset+9,offset+10,offset+11,offset+12,offset+13,offset+14,offset+15,offset+16,offset+17,offset+18,offset+19
        #如果向前找了20个交易日，仍然交易量不足20日，则判定长期停牌，直接剔除，到下面一个日期判断进行剔除也可以
#        print(stockCode,'长期停牌，剔除')
#        return
    
    #发现在2015年以前的股票，get_hist_data没有数据
    #所以不能再用这个数据，而是要自己去进行历史数据的处理，处理历史数据得到MA20
    
    #不能通过hist_data去获取数据了
    #stock_hist_data = tushare.get_hist_data(code=stockCode, start=firstOpenDay, end=ENDDATE)
    #存在一种特殊情况，就是类似601360,360借壳上市的情况
    #变更了股票代码，获取的hist_data周期与k_data不同的情况
    #需要进行判断并剔除
    
    
    if stock_k_data.empty:
        log.logger.info(stockCode, '为新上市股票，或存在停牌情况，进行剔除')
        return
    
    #print(stock_his.index)
    #type(stock_his.index)
    
    
    
    #第一行的偏移量
    #因为如果不是从当年第一个交易日开始，标号会有一个偏移量，在后续处理时，需要进行一个处理
    offset = stock_k_data.index[0]


    #{stock_his.at[0,'date']: 4098}
    
    holdShares = 0#持仓手数，1手为100股
    holdAvgPrice = 0#持仓的平均价格
    
    latestDealType = 0#最近一笔成交类型，-1表示卖出，1表示买入
    latestDealPrice = 0#最近一笔成交的价格
    
    totalInput = 0#累计总投入
    totalOutput = 0#累计总赎回
    
    biggestCashOccupy = 0#最大占用总金额，为totalInput-totalOutput的最大值
    
    #最大连续下跌/上涨计数：每当连续上涨/下跌超过上涨/下跌线，计数器加1
    #如果出现多次上涨后的下跌，则上涨计数归0
    #如果出现多次下跌后的上涨，则下跌计数归0
    #continuousFallCnt = 0
    #continuousRiseCnt = 0
    
    #最大连续上涨/下跌买入/卖出计数：连续上涨买入为正数，连续下跌卖出为负数，连续上涨超线买入，计数器加1，连续下跌超线卖出，计数器减1
    continuousRiseOrFallCnt = 0
    
    
    origStdout=sys.stdout  #保存标准输出流
    outputFile=strOutputDir +'/'+ stockCode + '.csv'
    #print(outputFile)
    sys.stdout = open(outputFile,'wt')
    
    printStockOutputHead(outputFile)
    
    
    
    stock_k_data.set_index('trade_date',drop=True, inplace=True)
    
    
    #按照交易日进行循环
    #而不是只根据股票交易数据进行循环
    #从firstOpenDay到enddate的交易日循环

    currday=firstDealDay

    closePriceToday=stock_k_data.at[currday,'close']

    #第一个交易日的处理，需要各个策略根据自身情况进行确定
    sharesToBuyOrSell,priceToBuyOrSell=strategy.getShareAndPriceToBuyOrSell(latestDealPrice, 
             latestDealType,holdShares,holdAvgPrice,
             continuousRiseOrFallCnt,stock_k_data,currday)

    if sharesToBuyOrSell>0:
        #如果判断为应当买入
        #更新持仓平均成本
        holdAvgPrice = (holdShares*holdAvgPrice+sharesToBuyOrSell*priceToBuyOrSell)/(holdShares+sharesToBuyOrSell)
        holdShares += sharesToBuyOrSell
        
        #获取买入交易费
        dealCharge=TradeChargeProcessor.getBuyCharge(sharesToBuyOrSell*100*priceToBuyOrSell)
        
        latestDealType=1
        latestDealPrice=priceToBuyOrSell
        totalInput+=sharesToBuyOrSell*priceToBuyOrSell*100+dealCharge
        netCashFlowToday=-(sharesToBuyOrSell*priceToBuyOrSell*100+dealCharge)
        
        returnValDF=printTradeInfo(outputFile,currday,1,closePriceToday,holdShares,
                                    holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                    latestDealType,latestDealPrice,dealCharge)
        
        biggestCashOccupy=totalInput
    else:
        #第一天不可能判断为卖出没有意义，没有份额可以卖出
        #既不需要买入，又不需要卖出
        #没有任何交易，打印对账信息:

        netCashFlowToday=0
        returnValDF=printTradeInfo(outputFile,currday,0,closePriceToday,holdShares,
                                    holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                    latestDealType,latestDealPrice,0)
    
    #获取下一自然日
    currday=sdProcessor.getNextCalDay(currday)
    
    
    
    #tic3=timer()
    #log.logger.info("tic3:%s"%(tic3))
    
    #用于传递最近一个交易日的数据
    #用于在非交易日继续重复输出
    lastDealDayDF=returnValDF
    
    while currday<=enddate:
        
        
        #tic32=timer()
        #log.logger.info("tic32:%s"%(tic32))
        
        if (not sdProcessor.isDealDay(currday)):
            #当前日期非交易日，需要输出一个空行到文件中
            #该空行内容与上一个交易日的内容相同
            dupLastLineinFile(currday,outputFile,lastDealDayDF)
            currday=StockDataProcessor.getNextCalDay(currday)
            continue

        
        try:
            closePriceToday=stock_k_data.at[currday,'close']
        except KeyError:
            #该交易日股票停牌
            #需要输出一个空行到文件中
            #该空行内容与上一个交易日的内容相同

            dupLastLineinFile(currday,outputFile,lastDealDayDF)
                        
            #如果直接停牌到endday
            #这样处理会导致无法输出数据到Summary文件中
            if currday==lastDealDay:
                #最后一个交易日的盈利情况
                latestProfit=lastDealDayDF.iat[0,10]
                printSummaryTradeInfo(summaryOutFile,stockCode,biggestCashOccupy,totalInput,totalOutput,
                                latestProfit,holdShares,closePriceToday)
            
            
            currday=StockDataProcessor.getNextCalDay(currday)
            continue
        
    

        #print("tic32-toc31:%s"%(tic32-toc31))
        sharesToBuyOrSell,priceToBuyOrSell=strategy.getShareAndPriceToBuyOrSell(latestDealPrice, 
                 latestDealType,holdShares,holdAvgPrice,
                 continuousRiseOrFallCnt,stock_k_data,currday)
        
        if sharesToBuyOrSell>0:
            #如果判断为下跌超线买入
            
            if continuousRiseOrFallCnt>=0:
                #此前一次是上涨超线卖出或未超线
                continuousRiseOrFallCnt=-1
            else:
                #此前就是下跌超线买入
                continuousRiseOrFallCnt=continuousRiseOrFallCnt-1

                
            #更新持仓平均成本
            holdAvgPrice=(holdShares*holdAvgPrice+sharesToBuyOrSell*priceToBuyOrSell)/(holdShares+sharesToBuyOrSell)
            holdShares+=sharesToBuyOrSell
            
            #获取买入交易费
            dealCharge=TradeChargeProcessor.getBuyCharge(sharesToBuyOrSell*100*priceToBuyOrSell)
            
            latestDealType=1
            latestDealPrice=priceToBuyOrSell
            totalInput+=sharesToBuyOrSell*priceToBuyOrSell*100+dealCharge
            netCashFlowToday=-(sharesToBuyOrSell*priceToBuyOrSell*100+dealCharge)
            
            returnValDF=printTradeInfo(outputFile,currday,1,closePriceToday,holdShares,
                                        holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                        latestDealType,latestDealPrice,dealCharge)
            
            if totalInput-totalOutput>biggestCashOccupy:
                biggestCashOccupy = totalInput - totalOutput
            
        elif sharesToBuyOrSell<0 and holdShares>=abs(sharesToBuyOrSell):
            #如果判断为应当卖出，而且确实有持仓可以卖出
            #如果已经没有持仓能够卖出，那就没有任何操作
            
            if continuousRiseOrFallCnt<=0:
                #此前一次是下跌超线买入或未超线
                continuousRiseOrFallCnt=1
            else:
                #此前就是上涨超线买入
                continuousRiseOrFallCnt=continuousRiseOrFallCnt+1

            if holdShares>abs(sharesToBuyOrSell):
                holdAvgPrice=(holdShares*holdAvgPrice-abs(sharesToBuyOrSell)*priceToBuyOrSell)/(holdShares-abs(sharesToBuyOrSell))
            else:
                holdAvgPrice=0
            holdShares -= abs(sharesToBuyOrSell)

        
            #获取卖出交易费
            dealCharge = TradeChargeProcessor.getSellCharge(abs(sharesToBuyOrSell)*100*priceToBuyOrSell)
                
            latestDealType = -1
            latestDealPrice = priceToBuyOrSell
            totalOutput += abs(sharesToBuyOrSell)*priceToBuyOrSell*100-dealCharge
            netCashFlowToday=abs(sharesToBuyOrSell)*priceToBuyOrSell*100-dealCharge
        
            if totalInput - totalOutput > biggestCashOccupy:
                biggestCashOccupy = totalInput - totalOutput
                    
            returnValDF=printTradeInfo(outputFile,currday,-1,closePriceToday,holdShares,
                                        holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                        latestDealType,latestDealPrice,dealCharge)
        
        else:
            #既不需要买入，又不需要卖出
            #没有任何交易，打印对账信息:
            netCashFlowToday=0
            returnValDF=printTradeInfo(outputFile,currday,0,closePriceToday,holdShares,
                                        holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                        latestDealType,latestDealPrice,0)
        
        
        lastDealDayDF=returnValDF
    
        #toc32=timer()
        #log.logger.info("toc32:%s"%(toc32))
        #log.logger.info("第3.2部分代码耗时:%s"%(toc32-tic32)) # 输出的时间，秒为单位
        

        #try:
        #    print("tic33-toc32:%s"%(tic33-toc32))
        #except UnboundLocalError:
        #    pass
        
        #tic33=timer()
        #log.logger.info("tic33:%s"%(tic33))
        
        if currday==lastDealDay:
            #跳出之前进行输出

            #outputFile = strOutputDir + '/Summary.csv'
            #sys.stdout = open(outputFile,'at+')
    
            #最后一个交易日的盈利情况
            latestProfit=returnValDF.iat[0,10]
            
            printSummaryTradeInfo(summaryOutFile,stockCode,biggestCashOccupy,totalInput,totalOutput,
                                  latestProfit,holdShares,closePriceToday)
            
            #log.logger.info("%s:持仓金额:%f"%(stockCode,round(holdShares*100*closePriceToday,2)))
        
            #sys.stdout=savedStdout  #恢复标准输出流
    
         
        #currday=StockDataProcessor.getNextDealDay(currday, False)
        currday=StockDataProcessor.getNextCalDay(currday)
        
        #toc33=timer()
        #log.logger.info("toc33:%s"%(toc33))
        #log.logger.info("第3.3部分代码耗时:%s"%(toc33-tic33)) # 输出的时间，秒为单位
        
        #log.logger.info("toc33-tic32:%s"%(toc33-tic32))
    
    
    sys.stdout=origStdout  #恢复标准输出流
    
    #toc3=timer()
    #log.logger.info("toc3:%s"%(toc3))
    #log.logger.info("第3部分代码耗时:%s"%(toc3-tic3)) # 输出的时间，秒为单位  


import argparse

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
    
    
    stockSelectStrategyList=[]#stockSelectStrategyString.split(',')
    tradeStrategyList=[]#tradeStrategyString.split(',')
    
    #生成选股策略
    if 'RawStrategy' in stockSelectStrategyString:
        stockSelectStrategyList.append(RawStrategy())
    if 'CashCowStrategy' in stockSelectStrategyString:
        stockSelectStrategyList.append(CashCowStrategy())
    if 'ROEStrategy' in stockSelectStrategyString:
        stockSelectStrategyList.append(ROEStrategy())
            
    #生成交易策略
    if 'BuylowSellhighStrategy' in tradeStrategyString:
        tradeStrategyList.append(BuylowSellhighStrategy())
    if 'SMAStrategy' in tradeStrategyString:
        tradeStrategyList.append(SMAStrategy())
    if 'MultiStepStrategy' in tradeStrategyString:
        tradeStrategyList.append(MultiStepStrategy())
    
    
    sdProcessor=StockDataProcessor()
    
    #通过STARTDATE找到第一个交易日
    firstDealDay=sdProcessor.getNextDealDay(startdate,True)
    
        
    #需要找到开始日期前面的20个交易日那天，从那一天开始获取数据
    #可能有企业临时停牌的问题，向前找20个交易日，有可能不够在后面扣除
    #向前找30个交易日

    twentyDaysBeforeFirstOpenDay=sdProcessor.getDealDayByOffset(firstDealDay, 30)
    
    for stockSelectStrategy in stockSelectStrategyList:
        
        print('开始处理选股策略:',stockSelectStrategy.getStrategyName())
        #从参数获取股票选取策略
        stockList=stockSelectStrategy.getSelectedStockList(sdProcessor,startdate)
        
        strOutterOutputDir=OUTPUTDIR+'/'+startdate+'-'+enddate+'-'+stockSelectStrategy.getStrategyName()
        
        myPath = Path(strOutterOutputDir)
        
        #如果该位置存在，则直接使用该位置，不用删除掉重新算
        if not(myPath.exists()):
            os.mkdir(strOutterOutputDir)
        #从参数获取交易策略
  
        
        #对所有策略进行循环：
        for tradeStrategy in tradeStrategyList:
            print('开始处理交易策略:',tradeStrategy.getStrategyName())
            #savedStdout = sys.stdout  #保存标准输出流
             
            strOutputDir=strOutterOutputDir+'/'+tradeStrategy.getStrategyName()
            
            myPath = Path(strOutputDir)
        
            #如果该位置存在，则直接使用该位置，不用删除掉重新算
            if not(myPath.exists()):
                os.mkdir(strOutputDir)
            
            summaryOutFile = strOutputDir+'/Summary.csv'
            
        
            myPath=Path(summaryOutFile)
            #如果Summary.csv已经存在，则直接追加即可，不用往里面继续写入抬头
            if not(myPath.exists()):
                #追加方式写入，针对如果已经处理了一半的策略
                #sys.stdout = open(outputFile,'at+')
                printSummaryOutputHead(summaryOutFile)
            #sys.stdout = savedStdout  #恢复标准输出流
        
            #对所有股票代码，循环进行处理
            #in_text = open(INPUTFILE, 'r')
            #直接对stockList进行遍历，不需要通过INPUTFILE获取股票列表
            
            #循环所有股票，使用指定策略进行处理
        #    for line in in_text.readlines():  
        #        stockCode = line.rstrip("\n")
            latestholdAmtDict={}
            
            for stockCode in stockList:
        
                outputFile = strOutputDir+'/'+ stockCode + '.csv'
                myPath = Path(outputFile)
        
                #如果文件已经存在，说明已经处理过了，直接跳过该股票即可
                if myPath.exists():
                    print(stockCode,'已处理过')
                    continue
                else:
                    lastDealDay=sdProcessor.getLastDealDay(enddate,True)
                    
                    processStock(sdProcessor,stockCode,tradeStrategy,strOutputDir,\
                                 firstDealDay,lastDealDay,twentyDaysBeforeFirstOpenDay,enddate,summaryOutFile)
                #    print('完成'+stockCode+'的处理')
        
        
            print('开始处理IRR计算')
            #读取结果文件列表
            stockfileList = os.listdir(strOutputDir)
            
            #记录csv内容的列表
            fileContentTupleList = []
            
            #对文件列表中的文件进行处理，获取内容列表
            for stockfileStr in stockfileList:
                if not 'Summary' in stockfileStr:
                    df = FileProcessor.readFile(strOutputDir+'/'+stockfileStr)
                    fileContentTupleList.append((stockfileStr[:-4],df))
        
        
        
            #对各个日期计算相应的资金净流量
            cashFlowDict= {}
            #对已有的内容列表进行处理
            for stockfileName,stockfileDF in fileContentTupleList:
                #print(stockfileName)
            
                #如果Summary-all.csv已经存在，则直接覆盖
              
                idx=0
                while True:
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
            
            savedStdout = sys.stdout  #保存标准输出流
            sys.stdout = open(strOutputDir+'/Summary-xirr.csv','wt+')
            
            cashFlowList=[]
            print('日期,当日发生资金净流量,截至当日资金净占用,当日收盘持仓总盈亏,当日收盘持仓总市值,持仓当日产生盈亏,持仓当日盈亏率')
            keysList=list(cashFlowDict.keys())
            keysList.sort()
            yesterdayTotalProfit=0

            for key in keysList:
                #日期
                print(key[0:4]+'/'+key[4:6]+'/'+key[6:8],end=',')
                #当日发生资金净流量
                print(cashFlowDict.get(key)[0],end=',')
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
                    print(0)
                else:
                    print(round(todayProfit/float(cashFlowDict.get(key)[3]),4))
                
                cashFlowList.append((datetime.date(int(key[0:4]),int(key[4:6]),int(key[6:8])),float(cashFlowDict.get(key)[0])))
            
            
            print(tradeStrategy.getStrategyName()+'在%s到%s期间内IRR为：'%(startdate,enddate),end=',')
            print(IRRProcessor.xirr2(cashFlowList))
            
            sys.stdout = savedStdout #恢复标准输出流

'''
Created on 2019年10月18日

@author: picc
'''
import tushare
import math
import sys
from pathlib import Path
import os
from com.xiaoda.stock.loopbacktester.utils.LoopbackTestUtils import LoopbackTestUtils
from com.xiaoda.stock.strategies.SimpleStrategy import SimpleStrategy
from com.xiaoda.stock.strategies.MultiStepStrategy import MultiStepStrategy
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import *
from datetime import datetime as dt
import datetime
import shutil
from com.xiaoda.stock.strategies.SMAStrategy import SMAStrategy


def printStockOutputHead():
    print('日期,交易类型,当天持仓账面总金额,当天持仓总手数,累计投入,累计赎回,当前持仓平均成本,\
    当天平均价格,当前持仓盈亏,最近一次交易类型,最近一次交易价格,当前全部投入回报率,本次交易手续费')

    
def printTradeInfo(date, dealType, avgPriceToday,holdShares,holdAvgPrice,
                   totalInput,totalOutput,latestDealType,latestDealPrice,dealCharge):
    print(date, end=',')
    print(dealType, end=',')
    print(round(avgPriceToday*holdShares*100,4), end=',')
    print(holdShares, end=',')
    print(round(totalInput,4), end=',')
    print(round(totalOutput,4), end=',')
    print(round(holdAvgPrice,4), end=',')
    print(round(avgPriceToday,4), end=',')
    currentProfit = round(avgPriceToday*holdShares*100+totalOutput-totalInput,4)
    print(currentProfit, end=',')
    print(latestDealType, end=',')
    print(round(latestDealPrice,4), end=',')
    totalProfitRate = currentProfit / totalInput * 100
    print(round(totalProfitRate,2), end='%,')
    print(round(dealCharge,2),end=', ')
    print()
    return [currentProfit,totalInput]


def printSummaryOutputHead():
    print('股票代码,最大资金占用,累计资金投入,最新盈亏,当前持仓金额')


'''
def readText():
    in_text = open(INPUTFILE, 'r')
    for line in in_text.readlines():
        stockCode = line.rstrip("\n")
        
    print('input OK!')
'''

#readText()



#具体处理股票的处理
def processStock(stockCode, strategy, strOutputDir, firstOpenDay):
    
    #返回的投入和盈利信息
    returnList=[];
    
    stock_k_data = tushare.get_k_data(code=stockCode,start=firstOpenDay,end=ENDDATE)

    if stock_k_data.empty:
        #如果没有任何返回值，说明该日期后没有上市交易过该股票
        print(stockCode, '无交易')
        return
    
    stock_hist_data = tushare.get_hist_data(code=stockCode, start=firstOpenDay, end=ENDDATE)
    #stock_k_data.at[0,'date']
    stock_hist_data = stock_hist_data.sort_index()
    #stock_hist_data.at['2018-01-02','open']


    '''
    print(type(stock_his))
    
    df = stock_his.head()
    
    print(df)
    print(df.dtypes)
    
    '''
    
    
    #print(stock_his.index)
    #type(stock_his.index)
    
    #第一行的偏移量
    #因为如果不是从当年第一个交易日开始，标号会有一个偏移量，在后续处理时，需要进行一个处理
    offset = stock_k_data.index[0]
    
    
    if stock_k_data.at[offset,'date'] > firstOpenDay:
        #对于不是从STARTDATE开始的新上市公司，进行剔除
        print(stockCode, '为新上市股票，或存在停牌情况，进行剔除')
        return
    
    #stock_his.shape[0]
    #print(stock_his)
    
    '''
    #print(stock_his.get_value(1, 'open', ))
    
    print(stock_his.at[0,'open'])
    
    print(stock_his.open)
    
    '''
    
    '''
    print(stock_his.iat[0,0])#查看具体位置数据
    
    print(stock_his.iloc[1])#查看某一行数据
    
    print(stock_his['open'])#查看某一列
    
    print(stock_his.shape[0])#查看行数
    
    print(stock_his.shape[1])#查看列数
    '''

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
    continuousFallCnt = 0
    continuousRiseCnt = 0
    
    #在获取的股票按天数据的索引
    i = 0


    savedStdout = sys.stdout  #保存标准输出流
    outputFile = strOutputDir + stockCode + '.csv'
    #print(outputFile)
    sys.stdout = open(outputFile,'wt')
    
    printStockOutputHead()
    
    while i<stock_k_data.shape[0]:
        #print(stock_his.iloc[i])
        avgPriceToday = (stock_k_data.at[i+offset,'open'] + stock_k_data.at[i+offset,'close'])/2
        todayDate = stock_k_data.at[i+offset,'date']


        
        if i==0:
            #第一个交易日，以当日均价买入n手
            holdShares = nShare
            holdAvgPrice = avgPriceToday
            
            #获取买入交易费
            dealCharge = LoopbackTestUtils.getBuyCharge(nShare*100*avgPriceToday)
            
            #最近一笔交易类型为买入，交易价格为当日均价
            latestDealType = 1
            latestDealPrice = avgPriceToday
            totalInput += holdShares*holdAvgPrice*100+dealCharge
            
    #        print(todayDate)
    #        print('完成买入交易，以%f价格买入%i手股票'%(avgPriceToday,nShare))
            returnList = printTradeInfo(todayDate, 1, avgPriceToday,holdShares,
                                        holdAvgPrice,totalInput,totalOutput,
                                        latestDealType,latestDealPrice,dealCharge)
            
            if totalInput - totalOutput > biggestCashOccupy:
                biggestCashOccupy = totalInput - totalOutput
        else:
            #不是第一个交易日
            #需要根据当前价格确定如何操作
            sharesToBuy = strategy.getShareToBuy(avgPriceToday,latestDealPrice, 
                     latestDealType,holdShares,holdAvgPrice,
                     continuousFallCnt,stock_hist_data,todayDate)
            
            sharesToSell = strategy.getShareToSell(avgPriceToday,latestDealPrice, 
                      latestDealType,holdShares,holdAvgPrice,
                      continuousRiseCnt,stock_hist_data,todayDate)
            
            if sharesToBuy>0:
                #如果判断为应当买入
                
                #出现了下跌超线的情况，下跌计数器增加1
                continuousFallCnt+=1
                
                if continuousRiseCnt>0:
                    #如果之前出现上涨超线
                    #把上涨计数器归0
                    continuousRiseCnt=0
                    
                #更新持仓平均成本
                holdAvgPrice = (holdShares*holdAvgPrice+sharesToBuy*avgPriceToday)/(holdShares+sharesToBuy)
                holdShares += sharesToBuy
                
                #获取买入交易费
                dealCharge = LoopbackTestUtils.getBuyCharge(sharesToBuy*100*avgPriceToday)
                
                latestDealType = 1
                latestDealPrice = avgPriceToday
                totalInput += sharesToBuy*avgPriceToday*100+dealCharge
                
                returnList = printTradeInfo(todayDate, 1, avgPriceToday,holdShares,
                                            holdAvgPrice,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,dealCharge)
                
                if totalInput - totalOutput > biggestCashOccupy:
                    biggestCashOccupy = totalInput - totalOutput
                
            elif sharesToSell>0 and holdShares>=sharesToSell:
                #如果判断为应当卖出，而且确实有持仓可以卖出
                #如果已经没有持仓能够卖出，那就没有任何操作
                
                #出现了上涨超线的情况，下跌计数器增加1
                continuousRiseCnt+=1
               
                if continuousFallCnt>0:
                    #如果之前出现下跌超线
                    #把下跌计数器归0
                    continuousFallCnt=0
                if holdShares>sharesToSell:
                    holdAvgPrice=(holdShares*holdAvgPrice-sharesToSell*avgPriceToday)/(holdShares-sharesToSell)
                else:
                    holdAvgPrice=0
                holdShares -= sharesToSell

            
                #获取卖出交易费
                dealCharge = LoopbackTestUtils.getSellCharge(sharesToSell*100*avgPriceToday)
                    
                latestDealType = -1
                latestDealPrice = avgPriceToday
                totalOutput += sharesToSell*avgPriceToday*100-dealCharge
                
            
                if totalInput - totalOutput > biggestCashOccupy:
                    biggestCashOccupy = totalInput - totalOutput
                
                returnList = printTradeInfo(todayDate, -1, avgPriceToday,holdShares,
                                            holdAvgPrice,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,dealCharge)
            
            else:
                #既不需要买入，又不需要卖出
                #没有任何交易，打印对账信息:
                returnList = printTradeInfo(todayDate, 0, avgPriceToday,holdShares,
                                            holdAvgPrice,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,0)
               
        i+=1
        
        if i==stock_k_data.shape[0]:
            #跳出之前进行输出

            outputFile = strOutputDir + 'Summary.csv'
            sys.stdout = open(outputFile,'a+')
    
            #最后一个交易日的盈利情况
            latestProfit = returnList[0]
            totalInput = returnList[1]
            
            print('\''+str(stockCode),end=', ')
            print(round(biggestCashOccupy,2), end=', ')
            print(round(totalInput,2), end=', ')
            print(latestProfit, end=', ')
            print(round(holdShares*100*avgPriceToday,2), end='\n')
            
            sys.stdout = savedStdout  #恢复标准输出流







#策略的列表
strList = [SMAStrategy("SMAStrategy"),SimpleStrategy("SimpleStrategy"),MultiStepStrategy('MultiStepStrategy')]


#通过STARTDATE找到第一个交易日
firstOpenDay = STARTDATE

while True:
    if tushare.is_holiday(firstOpenDay):
        #当前日期为节假日，查看下一天是否是交易日
        cday = dt.strptime(firstOpenDay, "%Y-%m-%d").date()
        dayOffset = datetime.timedelta(1)
        # 获取想要的日期的时间
        firstOpenDay = (cday + dayOffset).strftime('%Y-%m-%d')
    else:
        #找到第一个交易日，跳出
        break

#对所有策略进行循环：
for strategy in strList:
    
    savedStdout = sys.stdout  #保存标准输出流
    myPath = Path(OUTPUTDIR + '/'+strategy.getStrategyName()+'/')
    if myPath.exists():
        shutil.rmtree(OUTPUTDIR + '/'+strategy.getStrategyName()+'/')
    
    os.mkdir(OUTPUTDIR + '/'+strategy.getStrategyName()+'/')
    outputFile = OUTPUTDIR + '/'+strategy.getStrategyName()+'/'+'Summary.csv'
    sys.stdout = open(outputFile,'wt')
    printSummaryOutputHead()
    sys.stdout = savedStdout  #恢复标准输出流

    #对所有股票代码，循环进行处理
    in_text = open(INPUTFILE, 'r')
    
    #循环所有股票，使用指定策略进行处理
    for line in in_text.readlines():
        stockCode = line.rstrip("\n")
        processStock(stockCode, strategy, OUTPUTDIR + '/'+strategy.getStrategyName()+'/', firstOpenDay)
    #    print('完成'+stockCode+'的处理')



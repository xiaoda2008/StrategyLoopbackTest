'''
Created on 2019年10月18日

@author: picc
'''

import os
import sys
import shutil
from pathlib import Path
from datetime import datetime as dt
import datetime
from com.xiaoda.stock.strategies.SMAStrategy import SMAStrategy
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.strategies.SimpleStrategy import SimpleStrategy
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlUtils
from com.xiaoda.stock.strategies.MultiStepStrategy import MultiStepStrategy
from com.xiaoda.stock.loopbacktester.utils.LoopbackTestUtils import LoopbackTestUtils
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import STARTDATE,ENDDATE,nShare,LOGGINGDIR,OUTPUTDIR


def printStockOutputHead():
    print('日期,交易类型,当天持仓账面总金额,当天持仓总手数,累计投入,累计赎回,当天资金净占用,当前持仓平均成本,\
    当天平均价格,当前持仓盈亏,最近一次交易类型,最近一次交易价格,当前全部投入回报率,本次交易手续费')
    
def printTradeInfo(date, dealType, avgPriceToday,holdShares,holdAvgPrice,
                   totalInput,totalOutput,latestDealType,latestDealPrice,dealCharge):
    print(date, end=',')
    print(dealType, end=',')
    print(round(avgPriceToday*holdShares*100,4), end=',')
    print(holdShares, end=',')
    print(round(totalInput,4), end=',')
    print(round(totalOutput,4), end=',')
    print(round(totalInput-totalOutput,4), end=',')
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

def printSummaryTradeInfo(stockCode, biggestCashOccupy, totalInput,latestProfit,holdShares,avgPriceToday):
    print('\''+str(stockCode),end=', ')
    print(round(biggestCashOccupy,2), end=', ')
    print(round(totalInput,2), end=', ')
    print(latestProfit, end=', ')
    print(round(holdShares*100*avgPriceToday,2), end='\n')



#具体处理股票的处理
def processStock(stockCode, strategy, strOutputDir, firstOpenDay, twentyDaysBeforeFirstDay):
    
    #返回的投入和盈利信息
    returnList=[];
    
    stock_k_data = MysqlUtils.getStockKData(stockCode[:6],twentyDaysBeforeFirstDay,ENDDATE)

    if len(stock_k_data)==0:
        #如果没有任何返回值，说明该日期后没有上市交易过该股票
        print('%s在%s（前推20个交易日）到%s区间内无交易，剔除'%(stockCode,twentyDaysBeforeFirstDay,ENDDATE))
        return

    #stock_k_data.sort_index(inplace=True,ascending=False)

    #stock_k_data.reset_index(drop=True,inplace=True)

    
    stock_k_data['MA20'] = stock_k_data['close'].rolling(20).mean()
    
    offset = stock_k_data.index[0]
    #剔除掉向前找的20个交易日数据
    if stock_k_data.shape[0]>20:
        #print("Biggger than 20")
        stock_k_data = stock_k_data.drop([offset,offset+1,offset+2,offset+3])
        stock_k_data = stock_k_data.drop([offset+4,offset+5,offset+6,offset+7])
        stock_k_data = stock_k_data.drop([offset+8,offset+9,offset+10,offset+11])
        stock_k_data = stock_k_data.drop([offset+12,offset+13,offset+14,offset+15])
        stock_k_data = stock_k_data.drop([offset+16,offset+17,offset+18,offset+19])
    
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
    
    
    #第一行的偏移量
    #因为如果不是从当年第一个交易日开始，标号会有一个偏移量，在后续处理时，需要进行一个处理
    offset = stock_k_data.index[0]
    
    
    if stock_k_data.at[offset,'trade_date'] != firstOpenDay:
        #对于不是从STARTDATE开始的，可能是在前20个交易日有停牌，或者在STARTDATE后有停牌，进行剔除
        print(stockCode, '为新上市股票，或存在停牌情况，进行剔除')
        return


    
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
    
    
    #在获取的股票按天数据的索引
    i = 0


    savedStdout = sys.stdout  #保存标准输出流
    outputFile = strOutputDir + stockCode + '.csv'
    #print(outputFile)
    sys.stdout = open(outputFile,'wt')
    
    printStockOutputHead()
    
    while i<stock_k_data.shape[0]:
        #print(stock_his.iloc[i])
        openPriceToday = (stock_k_data.at[i+offset,'open'] + stock_k_data.at[i+offset,'close'])/2
        todayDate = stock_k_data.at[i+offset,'trade_date']


        
        if i==0:
            #第一个交易日，以当日均价买入n手
            holdShares = nShare
            holdAvgPrice = openPriceToday
            
            #获取买入交易费
            dealCharge = LoopbackTestUtils.getBuyCharge(nShare*100*openPriceToday)
            
            #最近一笔交易类型为买入，交易价格为当日均价
            latestDealType = 1
            latestDealPrice = openPriceToday
            totalInput += holdShares*holdAvgPrice*100+dealCharge
            
    #        print(todayDate)
    #        print('完成买入交易，以%f价格买入%i手股票'%(avgPriceToday,nShare))
            returnList = printTradeInfo(todayDate, 1, openPriceToday,holdShares,
                                        holdAvgPrice,totalInput,totalOutput,
                                        latestDealType,latestDealPrice,dealCharge)
            
            if totalInput - totalOutput > biggestCashOccupy:
                biggestCashOccupy = totalInput - totalOutput
        else:
            #不是第一个交易日
            #需要根据当前价格确定如何操作
                     
            sharesToBuyOrSell = strategy.getShareToBuyOrSell(openPriceToday,latestDealPrice, 
                     latestDealType,holdShares,holdAvgPrice,
                     continuousRiseOrFallCnt,stock_k_data,todayDate)
            
            if sharesToBuyOrSell>0:
                #如果判断为下跌超线买入
                
                if continuousRiseOrFallCnt>=0:
                    #此前一次是上涨超线卖出或未超线
                    continuousRiseOrFallCnt=-1
                else:
                    #此前就是下跌超线买入
                    continuousRiseOrFallCnt=continuousRiseOrFallCnt-1


                #更新持仓平均成本
                holdAvgPrice = (holdShares*holdAvgPrice+sharesToBuyOrSell*openPriceToday)/(holdShares+sharesToBuyOrSell)
                holdShares += sharesToBuyOrSell
                
                #获取买入交易费
                dealCharge = LoopbackTestUtils.getBuyCharge(sharesToBuyOrSell*100*openPriceToday)
                
                latestDealType = 1
                latestDealPrice = openPriceToday
                totalInput += sharesToBuyOrSell*openPriceToday*100+dealCharge
                
                returnList = printTradeInfo(todayDate, 1, openPriceToday,holdShares,
                                            holdAvgPrice,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,dealCharge)
                
                if totalInput - totalOutput > biggestCashOccupy:
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
                    holdAvgPrice=(holdShares*holdAvgPrice-abs(sharesToBuyOrSell)*openPriceToday)/(holdShares-abs(sharesToBuyOrSell))
                else:
                    holdAvgPrice=0
                holdShares -= abs(sharesToBuyOrSell)

            
                #获取卖出交易费
                dealCharge = LoopbackTestUtils.getSellCharge(abs(sharesToBuyOrSell)*100*openPriceToday)
                    
                latestDealType = -1
                latestDealPrice = openPriceToday
                totalOutput += abs(sharesToBuyOrSell)*openPriceToday*100-dealCharge
                
            
                if totalInput - totalOutput > biggestCashOccupy:
                    biggestCashOccupy = totalInput - totalOutput
                
                returnList = printTradeInfo(todayDate, -1, openPriceToday,holdShares,
                                            holdAvgPrice,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,dealCharge)
            
            else:
                #既不需要买入，又不需要卖出
                #没有任何交易，打印对账信息:
                returnList = printTradeInfo(todayDate, 0, openPriceToday,holdShares,
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
            
            printSummaryTradeInfo(stockCode, biggestCashOccupy, totalInput,
                                  latestProfit,holdShares,openPriceToday)
            
            sys.stdout = savedStdout  #恢复标准输出流






#通过STARTDATE找到第一个交易日
firstOpenDay = STARTDATE

log = Logger(LOGGINGDIR+'/'+os.path.split(__file__)[-1].split(".")[0] + '.log',level='debug')

'''
log.logger.debug('debug')
log.logger.info('info')
log.logger.warning('警告')
log.logger.error('报错')
log.logger.critical('严重')
'''
'''
while True:
    if MysqlUtils.isMarketDay(datetime.strptime(firstOpenDay,"%Y%m%d").date().strftime('%Y%m%d')):
        #找到第一个交易日，跳出
        break
    else:
        #当前日期为节假日，查看下一天是否是交易日
        cday = datetime.strptime(firstOpenDay, "%Y%m%d").date()
        dayOffset = datetime.timedelta(1)
        # 获取想要的日期的时间
        firstOpenDay = (cday + dayOffset).strftime('%Y%m%d')
'''
#如果开始日期不是交易日，找到下一个交易日
if not(MysqlUtils.isMarketDay(firstOpenDay)):
    firstOpenDay = MysqlUtils.getNextMarketDay(firstOpenDay)
    
#需要找到开始日期前面的20个交易日那天，从那一天开始获取数据
cday = dt.strptime(firstOpenDay, "%Y%m%d").date()
dayOffset = datetime.timedelta(1)
cnt=0
# 获取想要的日期的时间
while True:
    cday = (cday - dayOffset)
    if MysqlUtils.isMarketDay(cday.strftime('%Y%m%d')):
        cnt+=1
        if cnt==20:
            break
twentyDaysBeforeFirstOpenDay=cday.strftime('%Y%m%d')


strOutterOutputDir=OUTPUTDIR+'/'+STARTDATE+'-'+ENDDATE

myPath = Path(strOutterOutputDir)

if myPath.exists():
    shutil.rmtree(strOutterOutputDir)

os.mkdir(strOutterOutputDir)

'''
#使用TuShare pro版本
tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')

sdDataAPI = tushare.pro_api()
'''


#获取股票列表
#sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
stockCodeList = MysqlUtils.getStockList()



#策略的列表
stgList = [SMAStrategy("SMAStrategy"),SimpleStrategy("SimpleStrategy"),MultiStepStrategy('MultiStepStrategy')]

#对所有策略进行循环：
for strategy in stgList:
    savedStdout = sys.stdout  #保存标准输出流
     
    strOutputDir=strOutterOutputDir+'/'+strategy.getStrategyName()+'/'
    
    os.mkdir(strOutputDir)
    
    outputFile = strOutputDir+'/Summary.csv'
    
    
    sys.stdout = open(outputFile,'wt')
    printSummaryOutputHead()
    sys.stdout = savedStdout  #恢复标准输出流

    #对所有股票代码，循环进行处理
    #in_text = open(INPUTFILE, 'r')
    #直接对stockList进行遍历，不需要通过INPUTFILE获取股票列表
    
    #循环所有股票，使用指定策略进行处理
    for stockCode in stockCodeList:

        processStock(stockCode.ts_code,strategy,strOutputDir,firstOpenDay,twentyDaysBeforeFirstOpenDay)
    #    print('完成'+stockCode+'的处理')


#1、可以把数据下载到本地，对每支股票的分析，从mysql数据库获取数据，而不是每个都要到远程获取-》基本完成，20191104
#2、需要注意，MA需要按照当日开盘价计算，而不应该用当日平均价，且MA应当用前一天的MA，不应该用当日MA
#3、需要注意，在有涨停、跌停的日子，无法以涨停、跌停价进行相关交易
#4、增加代码，在完成所有股票的输出以后，对输出进行测试，计算每天的资金占用，按照日期为维度进行一定的分析，可以增加一个按日收益率汇总、按天资金占用情况，对比各种策略
#5、根据实际的资金进出，进行IRR计算
#6、计算结果也可以输出到数据库，而不是输出到文件，方便后续进行处理
#7、对于SMA策略，第一个交易日可以不用买入，而是满足条件之后才买入


#最终应该是一个Tester：回测，一个Simulator：模拟交易，一个Monitor：真实交易的辅助监测

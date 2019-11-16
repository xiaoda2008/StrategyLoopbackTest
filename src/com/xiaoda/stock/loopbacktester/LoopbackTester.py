'''
Created on 2019年10月18日

@author: picc
'''
#import tushare
#import math
import sys
from pathlib import Path
import os
from com.xiaoda.stock.loopbacktester.utils.ChargeUtils import ChargeProcessor
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import STARTDATE,ENDDATE,OUTPUTDIR
from datetime import datetime as dt
import datetime
#import shutil
from sqlalchemy.util.langhelpers import NoneType
from com.xiaoda.stock.loopbacktester.utils.FileUtils import FileProcessor
from com.xiaoda.stock.loopbacktester.utils.IRRUtils import IRRProcessor
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor


from com.xiaoda.stock.loopbacktester.strategy.trade.SimpleStrategy import SimpleStrategy
from com.xiaoda.stock.loopbacktester.strategy.trade.MultiStepStrategy import MultiStepStrategy
from com.xiaoda.stock.loopbacktester.strategy.trade.SMAStrategy import SMAStrategy

from com.xiaoda.stock.loopbacktester.strategy.stockselect.RawStrategy import RawStrategy
from com.xiaoda.stock.loopbacktester.strategy.stockselect.CashCowStrategy import CashCowStrategy


def printStockOutputHead():
    print('日期,交易类型,当天持仓账面总金额,当天持仓总手数,累计投入,累计赎回,当天资金净占用,当天资金净流量,当前持仓平均成本,\
    当天平均价格,当前持仓盈亏,最近一次交易类型,最近一次交易价格,当前全部投入回报率,本次交易手续费')
    
def printTradeInfo(date, dealType, avgPriceToday,holdShares,holdAvgPrice,netCashFlowToday,
                   totalInput,totalOutput,latestDealType,latestDealPrice,dealCharge):
    print(date, end=',')
    print(dealType, end=',')
    print(round(avgPriceToday*holdShares*100,4), end=',')
    print(holdShares, end=',')
    print(round(totalInput,4), end=',')
    print(round(totalOutput,4), end=',')
    print(round(totalInput-totalOutput,4), end=',')
    print(round(netCashFlowToday,4),end=',')
    
    print(round(holdAvgPrice,4), end=',')
    print(round(avgPriceToday,4), end=',')
    currentProfit = round(avgPriceToday*holdShares*100+totalOutput-totalInput,4)
    print(currentProfit, end=',')
    print(latestDealType, end=',')
    print(round(latestDealPrice,4), end=',')
    if totalInput>0:
        totalProfitRate=currentProfit/totalInput * 100
    else:
        totalProfitRate=0
    print(round(totalProfitRate,2), end='%,')
    print(round(dealCharge,2),end='\n')
    #return [currentProfit,totalInput,totalOutput]
    return currentProfit

def printSummaryOutputHead():
    print('股票代码,最大资金占用,累计资金投入,累计资金赎回,最新盈亏,当前持仓金额')

def printSummaryTradeInfo(stockCode, biggestCashOccupy, totalInput,totalOutput,latestProfit,holdShares,avgPriceToday):
    print('\''+str(stockCode),end=', ')
    print(round(biggestCashOccupy,2), end=', ')
    print(round(totalInput,2), end=', ')
    print(round(totalOutput,2), end=', ')    
    print(latestProfit, end=', ')
    print(round(holdShares*100*avgPriceToday,2), end='\n')

'''
def readText():
    in_text = open(INPUTFILE, 'r')
    for line in in_text.readlines():
        stockCode = line.rstrip("\n")
        
    print('input OK!')
'''

#readText()



#具体处理股票的处理
def processStock(stockCode, strategy, strOutputDir, firstOpenDay, twentyDaysBeforeFirstDay):
    
    
    #stock_k_data = tushare.pro_bar(ts_code=stockCode,adj='qfq',
    #                               start_date=twentyDaysBeforeFirstDay,end_date=ENDDATE)
    #time.sleep(0.31)
    
    stock_k_data=MysqlProcessor.getStockKData(stockCode, twentyDaysBeforeFirstDay, ENDDATE)
    
    #sprint(stock_k_data.columns)

    if type(stock_k_data)==NoneType or stock_k_data.empty:
        #如果没有任何返回值，说明该日期后没有上市交易过该股票
        print('%s在%s（前推30个交易日）到%s区间内无交易，剔除'%(stockCode,twentyDaysBeforeFirstDay,ENDDATE))
        return

#    stock_k_data = tushare.get_k_data(code=stockCode,start=twentyDaysBeforeFirstDay,end=ENDDATE)
    #stock_k_data.sort_index(inplace=True,ascending=True)

    stock_k_data.reset_index(drop=True,inplace=True)

    #错位一下，以便计算的MA20不包含当天的close数据
    #stock_k_data['close_shift']=stock_k_data['close'].shift(1)
    #直接用pre_close即可，不用计算

    #计算出MA20的数据，问题在于，这个MA20是包含当前天的，有些问题，应当不包含当前天
    stock_k_data['yesterday_MA20'] = stock_k_data['pre_close'].rolling(20).mean()
    stock_k_data['today_MA20'] = stock_k_data['close'].rolling(20).mean()
    
    
    offset = stock_k_data.index[0]
    
    #剔除掉向前找的20个交易日数据
    #不要这样剔除，而是一直剔除到firstOpenDay
    
    a=0
    while True:
        
        #删空了，在向前推进的交易日有交易，但在查询区间内股票就没有交易
        if stock_k_data.empty:
            break
        
        #允许正好这一天股票停牌
        if stock_k_data.at[a+offset,'trade_date']>=firstOpenDay:
            break
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
    
    
    #stock_k_data.at[0,'date']
    #stock_hist_data = stock_hist_data.sort_index()
    #stock_hist_data.at['2018-01-02','open']


    '''
    print(type(stock_his))
    
    df = stock_his.head()
    
    print(df)
    print(df.dtypes)
    
    '''
    
    if stock_k_data.empty:
        print(stockCode, '为新上市股票，或存在停牌情况，进行剔除')
        return
    
    #print(stock_his.index)
    #type(stock_his.index)
    
    #第一行的偏移量
    #因为如果不是从当年第一个交易日开始，标号会有一个偏移量，在后续处理时，需要进行一个处理
    offset = stock_k_data.index[0]

    
    '''
    if stock_hist_data.shape[0] < stock_k_data.shape[0]:
        print(stockCode, '存在借壳上市情况，进行剔除')
        return
    '''
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
    #continuousFallCnt = 0
    #continuousRiseCnt = 0
    
    #最大连续上涨/下跌买入/卖出计数：连续上涨买入为正数，连续下跌卖出为负数，连续上涨超线买入，计数器加1，连续下跌超线卖出，计数器减1
    continuousRiseOrFallCnt = 0
    
    
    #在获取的股票按天数据的索引
    i = 0


    savedStdout = sys.stdout  #保存标准输出流
    outputFile = strOutputDir +'/'+ stockCode + '.csv'
    #print(outputFile)
    sys.stdout = open(outputFile,'wt')
    
    printStockOutputHead()
    
    while i<stock_k_data.shape[0]:
        #print(stock_his.iloc[i])
        avgPriceToday = (float(stock_k_data.at[i+offset,'open'])+float(stock_k_data.at[i+offset,'close']))/2
        todayDate = stock_k_data.at[i+offset,'trade_date']


        
        if i==0:
            #第一个交易日的处理，需要各个策略根据自身情况进行确定
            sharesToBuyOrSell = strategy.getShareToBuyOrSell(avgPriceToday,latestDealPrice, 
                     latestDealType,holdShares,holdAvgPrice,
                     continuousRiseOrFallCnt,stock_k_data,todayDate)
            
            if sharesToBuyOrSell>0:
                #如果判断为应当买入
                #更新持仓平均成本
                holdAvgPrice = (holdShares*holdAvgPrice+sharesToBuyOrSell*avgPriceToday)/(holdShares+sharesToBuyOrSell)
                holdShares += sharesToBuyOrSell
                
                #获取买入交易费
                dealCharge = ChargeProcessor.getBuyCharge(sharesToBuyOrSell*100*avgPriceToday)
                
                latestDealType = 1
                latestDealPrice = avgPriceToday
                totalInput += sharesToBuyOrSell*avgPriceToday*100+dealCharge
                netCashFlowToday = -(sharesToBuyOrSell*avgPriceToday*100+dealCharge)
                
                returnVal = printTradeInfo(todayDate, 1, avgPriceToday,holdShares,
                                            holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,dealCharge)
                
                biggestCashOccupy = totalInput
            else:
                #第一天判断为卖出没有意义，没有份额可以卖出
                #既不需要买入，又不需要卖出
                #没有任何交易，打印对账信息:

                netCashFlowToday=0
                returnVal = printTradeInfo(todayDate, 0, avgPriceToday,holdShares,
                                            holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,0)

        else:
            #不是第一个交易日
            #需要根据当前价格确定如何操作
                     
            sharesToBuyOrSell = strategy.getShareToBuyOrSell(avgPriceToday,latestDealPrice, 
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
                holdAvgPrice = (holdShares*holdAvgPrice+sharesToBuyOrSell*avgPriceToday)/(holdShares+sharesToBuyOrSell)
                holdShares += sharesToBuyOrSell
                
                #获取买入交易费
                dealCharge = ChargeProcessor.getBuyCharge(sharesToBuyOrSell*100*avgPriceToday)
                
                latestDealType = 1
                latestDealPrice = avgPriceToday
                totalInput += sharesToBuyOrSell*avgPriceToday*100+dealCharge
                netCashFlowToday = -(sharesToBuyOrSell*avgPriceToday*100+dealCharge)
                
                returnVal = printTradeInfo(todayDate, 1, avgPriceToday,holdShares,
                                            holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
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
                    holdAvgPrice=(holdShares*holdAvgPrice-abs(sharesToBuyOrSell)*avgPriceToday)/(holdShares-abs(sharesToBuyOrSell))
                else:
                    holdAvgPrice=0
                holdShares -= abs(sharesToBuyOrSell)

            
                #获取卖出交易费
                dealCharge = ChargeProcessor.getSellCharge(abs(sharesToBuyOrSell)*100*avgPriceToday)
                    
                latestDealType = -1
                latestDealPrice = avgPriceToday
                totalOutput += abs(sharesToBuyOrSell)*avgPriceToday*100-dealCharge
                netCashFlowToday=abs(sharesToBuyOrSell)*avgPriceToday*100-dealCharge
            
                if totalInput - totalOutput > biggestCashOccupy:
                    biggestCashOccupy = totalInput - totalOutput
                        
                returnVal = printTradeInfo(todayDate, -1, avgPriceToday,holdShares,
                                            holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,dealCharge)
            
            else:
                #既不需要买入，又不需要卖出
                #没有任何交易，打印对账信息:
                netCashFlowToday=0
                returnVal = printTradeInfo(todayDate, 0, avgPriceToday,holdShares,
                                            holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,0)
               
        i+=1
        
        if i==stock_k_data.shape[0]:
            #跳出之前进行输出

            outputFile = strOutputDir + '/Summary.csv'
            sys.stdout = open(outputFile,'at+')
    
            #最后一个交易日的盈利情况
            latestProfit = returnVal
            
            printSummaryTradeInfo(stockCode, biggestCashOccupy, totalInput,totalOutput,
                                  latestProfit,holdShares,avgPriceToday)
            
            sys.stdout = savedStdout  #恢复标准输出流



import getopt
import argparse

if __name__ == '__main__':
    
    # 创建命令行解析器句柄，并自定义描述信息
    parser = argparse.ArgumentParser(description="test the argparse package")
    # 定义必选参数 positionArg
   # parser.add_argument("project_name")
    # 定义可选参数module
    parser.add_argument("--stockstrategy","-ss",type=str, default=1,help="Select the stock select strategy")
    # 定义可选参数module1
    parser.add_argument("--tradestrategy", "-ts",type=str, default=1,help="Select the trade strategy")
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
    
    for k, v in params.items():
        if k=='stockstrategy':
            stockSelectStrategyString=v
        elif k=='tradestrategy':
            tradeStrategyString=v
        #v1.append(v)
        # print(v)

    #print(stockSelectStrategyString)
    #print(tradeStrategyString)
    
    
    
    stockSelectStrategyList=[]#stockSelectStrategyString.split(',')
    tradeStrategyList=[]#tradeStrategyString.split(',')
    
    #生成选股策略
    if 'RawStrategy' in stockSelectStrategyString:
        stockSelectStrategyList.append(RawStrategy('RawStrategy'))
    if 'CashCowStrategy' in stockSelectStrategyString:
        stockSelectStrategyList.append(CashCowStrategy('CashCowStrategy'))
    
    #生成交易策略
    if 'SimpleStrategy' in tradeStrategyString:
        tradeStrategyList.append(SimpleStrategy('SimpleStrategy'))
    if 'SMAStrategy' in tradeStrategyString:
        tradeStrategyList.append(SMAStrategy('SMAStrategy'))
    if 'MultiStepStrategy' in tradeStrategyString:
        tradeStrategyList.append(MultiStepStrategy('MultiStepStrategy'))
    
    #使用TuShare pro版本
    #tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    
    #sdDataAPI = tushare.pro_api()
    
    
    #通过STARTDATE找到第一个交易日
    firstOpenDay = STARTDATE
    
    
    trade_cal_data=MysqlProcessor.getTradeCal()
    #trade_cal_data = sdDataAPI.trade_cal(start_date='1990-12-19')
    
    trade_cal_data=trade_cal_data.set_index('cal_date')
    
    while True:
        if trade_cal_data.at[dt.strptime(firstOpenDay, "%Y%m%d").date().strftime('%Y%m%d'),'is_open']==0:
            #tushare.is_holiday(dt.strptime(firstOpenDay, "%Y%m%d").date().strftime('%Y%m%d')):
            #当前日期为节假日，查看下一天是否是交易日
            cday = dt.strptime(firstOpenDay, "%Y%m%d").date()
            dayOffset = datetime.timedelta(1)
            # 获取想要的日期的时间
            firstOpenDay = (cday + dayOffset).strftime('%Y%m%d')
        else:
            #找到第一个交易日，跳出
            break
    
        
    #需要找到开始日期前面的20个交易日那天，从那一天开始获取数据
    #可能有企业临时停牌的问题，向前找20个交易日，有可能不够在后面扣除
    #向前找30个交易日
    
    
    cday = dt.strptime(firstOpenDay, "%Y%m%d").date()
    dayOffset = datetime.timedelta(1)
    cnt=0
    # 获取想要的日期的时间
    while True:
        cday = (cday - dayOffset)
        if trade_cal_data.at[cday.strftime('%Y%m%d'),'is_open']==1:
            cnt+=1
            if cnt==30:
                break
    twentyDaysBeforeFirstOpenDay=cday.strftime('%Y%m%d')
    
    
    '''
    if myPath.exists():
        shutil.rmtree(strOutterOutputDir)
    
    os.mkdir(strOutterOutputDir)
    '''
    
    '''
    df = tushare.get_stock_basics()
    df.columns
    for nm in df.name:
        if 'ST' in nm:
            print('ST')
    '''
    
    '''
    sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    
    
    stockDict={}
    
    for idx in sdf.index:
        stockDict[sdf.at[idx,'ts_code']]=sdf.at[idx,'name']
        
    '''
    
    #stockCodeList = sdf['ts_code']
    
    for stockSelectStrategy in stockSelectStrategyList:
        
        #从参数获取股票选取策略
        stockList=stockSelectStrategy.getSelectedStockList(STARTDATE)
        
        strOutterOutputDir=OUTPUTDIR+'/'+STARTDATE+'-'+ENDDATE+'-'+stockSelectStrategy.getStrategyName()
        
        myPath = Path(strOutterOutputDir)
        
        #如果该位置存在，则直接使用该位置，不用删除掉重新算
        if not(myPath.exists()):
            os.mkdir(strOutterOutputDir)
        #从参数获取交易策略
    #    strategyList= [SMAStrategy("SMAStrategy"),SimpleStrategy("SimpleStrategy"),MultiStepStrategy('MultiStepStrategy')]
        #strList= [SimpleStrategy("SimpleStrategy"),MultiStepStrategy('MultiStepStrategy')]
        
        
        
        #对所有策略进行循环：
        for tradeStrategy in tradeStrategyList:
            savedStdout = sys.stdout  #保存标准输出流
             
            strOutputDir=strOutterOutputDir+'/'+tradeStrategy.getStrategyName()
            
            myPath = Path(strOutputDir)
        
            #如果该位置存在，则直接使用该位置，不用删除掉重新算
            if not(myPath.exists()):
                os.mkdir(strOutputDir)
            
            outputFile = strOutputDir+'/Summary.csv'
            
        
            myPath = Path(outputFile)
            #如果Summary.csv已经存在，则直接追加即可，不用往里面继续写入抬头
            if not(myPath.exists()):
                #追加方式写入，针对如果已经处理了一半的策略
                sys.stdout = open(outputFile,'at+')
                printSummaryOutputHead()
            sys.stdout = savedStdout  #恢复标准输出流
        
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
                    processStock(stockCode,tradeStrategy,strOutputDir,firstOpenDay,twentyDaysBeforeFirstOpenDay)
                #    print('完成'+stockCode+'的处理')
        
        
        
            #读取文件列表
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
                print(stockfileName)
            
                #如果Summary-all.csv已经存在，则直接覆盖
              
                i=0
                while True:
                    if not (stockfileDF.at[i,'日期'] in cashFlowDict):
                        cashFlowDict[stockfileDF.at[i,'日期']]=float(stockfileDF.at[i,'当天资金净流量'])
                    else:    
                        cashFlowDict[stockfileDF.at[i,'日期']]=float(cashFlowDict[stockfileDF.at[i,'日期']])+float(stockfileDF.at[i,'当天资金净流量'])
                    i=i+1
                    if i==len(stockfileDF):
                        #最后一天，要把当天的持仓增加到净现金流
                        #以便计算XIRR
                        cashFlowDict[stockfileDF.at[i-1,'日期']]=float(cashFlowDict[stockfileDF.at[i-1,'日期']])+float(stockfileDF.at[i-1,'当天持仓账面总金额'])
                        break
            
            #对字典进行一下排序
            sorted(cashFlowDict)
            
            savedStdout = sys.stdout  #保存标准输出流
            sys.stdout = open(strOutputDir+'/Summary-xirr.csv','wt+')
            
            cashFlowList=[]
            print('日期,当日资金净流量')
            keysList=list(cashFlowDict.keys())
            keysList.sort()
            for key in keysList:
                print(key[0:4]+'/'+key[4:6]+'/'+key[6:8],end=',')
                print(cashFlowDict.get(key))
                cashFlowList.append((datetime.date(int(key[0:4]),int(key[4:6]),int(key[6:8])),float(cashFlowDict.get(key))))
            
            
            print(tradeStrategy.getStrategyName()+'在%s到%s期间内IRR为：'%(STARTDATE,ENDDATE),end=',')
            print(IRRProcessor.xirr2(cashFlowList))
            
            sys.stdout = savedStdout #恢复标准输出流

'''
Created on 2019年10月18日

@author: picc
'''
import tushare
import math
from datetime import datetime as dt
import pandas
import datetime
import numpy

date_string = "2018-01-01"

cday = dt.strptime(date_string, "%Y-%m-%d").date()

offset = datetime.timedelta(1)

# 获取想要的日期的时间
re_date = (cday + offset).strftime('%Y-%m-%d')



print(re_date)


#d:/stockList.txt
#股票代码库
INPUTFILE = 'D:/stockList.txt'
OUTPUTDIR = 'D:/outputDir/'

def printHead():
    print('日期,交易类型,当天持仓账面总金额,当天持仓总手数,累计投入,累计赎回,当前持仓平均成本,当天平均价格,当前持仓盈亏,最近一次交易类型,最近一次交易价格,当前全部投入回报率')
    
def printTradeInfo(date, dealType, avgPriceToday,holdShares,holdAvgPrice,totalInput,totalOutput,latestDealType,latestDealPrice):
    print(date, end=',')
    print(dealType, end=',')
    print(round(avgPriceToday*holdShares * 100,4), end=',')
    print(holdShares, end=',')
    print(round(totalInput*100,4), end=',')
    print(round(totalOutput*100,4), end=',')
    print(round(holdAvgPrice,4), end=',')
    print(round(avgPriceToday,4), end=',')
    currentProfit = round(avgPriceToday*holdShares*100+totalOutput*100-totalInput*100,4)
    print(currentProfit, end=',')
    print(latestDealType, end=',')
    print(round(latestDealPrice,4), end=',')
    totalProfitRate = currentProfit / (totalInput*100) * 100
    print(round(totalProfitRate,2), end='%,')
    print()








stock_his = tushare.get_k_data('000001',start='2018-10-16',end='')



stock_his.set_index('date',inplace=True)


stock_his['SMA_20'] = stock_his['close'].rolling(20).mean()


stock_his['close_shift'] = stock_his['close'].shift(1)

stock_his.plot(subplots=True,figsize=(10,6))

stock_his[['close','SMA_20']].plot(figsize=(10,6))

print()

s = pandas.Series([20, 21, 12], index=['London', 'New York', 'Helsinki'])


def add_custom_values(x, **kwargs):
    for month in kwargs:
        x += kwargs[month]
    return x
s.apply(add_custom_values, june=30, july=20, august=25)

#s.apply(numpy.log)



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
offset = stock_his.index[0]

#stock_his.shape[0]

#    print(stock_his)

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


nShare = 10 #每次交易单位，默认为10手
#{stock_his.at[0,'date']: 4098}

holdShares = 0#持仓手数，1手为100股
holdAvgPrice = 0#持仓的平均价格

latestDealType = 0#最近一笔成交类型，-1表示卖出，1表示买入
latestDealPrice = 0#最近一笔成交的价格



totalInput = 0#累计总投入
totalOutput = 0#累计总赎回


biggestCashOccupy = 0#最大占用总金额，为totalInput-totalOutput的最大值

i = 0

import sys
savedStdout = sys.stdout  #保存标准输出流
outputFile = OUTPUTDIR + '000029' + '.csv'
print(outputFile)
sys.stdout = open(outputFile,'wt')

printHead()

while i<stock_his.shape[0]:
#    print(stock_his.iloc[i])

    avgPriceToday = (stock_his.at[i+offset,'open'] + stock_his.at[i+offset,'close'])/2
    todayDate = stock_his.at[i+offset,'date']
    if i==0:
        #第一个交易日，以当日均价买入n手
        holdShares = nShare
        holdAvgPrice = avgPriceToday
        #最近一笔交易类型为买入，交易价格为当日均价
        latestDealType = 1
        latestDealPrice = avgPriceToday
        totalInput += holdShares * holdAvgPrice
#        print(todayDate)
#        print('完成买入交易，以%f价格买入%i手股票'%(avgPriceToday,nShare))
        printTradeInfo(todayDate, 1, avgPriceToday,holdShares,holdAvgPrice,totalInput,totalOutput,latestDealType,latestDealPrice)
        
        if 100*(totalInput - totalOutput) > biggestCashOccupy:
            biggestCashOccupy = 100*(totalInput - totalOutput)
    else:
        #不是第一个交易日
        #需要根据当前价格确定如何操作
        if avgPriceToday <= 0.85*latestDealPrice:
            #价格小于上次交易价格0.85，买入nShare/2手下取整
            buyShare = math.floor(nShare/2)
            holdAvgPrice = (holdShares*holdAvgPrice + buyShare*avgPriceToday)/(holdShares+buyShare)
            holdShares += buyShare
            
            latestDealType = 1
            latestDealPrice = avgPriceToday
            totalInput += buyShare*avgPriceToday
#            print(todayDate)
#            print('完成买入交易，以%f价格买入%i手股票'%(round(avgPriceToday,2),buyShare))
            printTradeInfo(todayDate, 1, avgPriceToday,holdShares,holdAvgPrice,totalInput,totalOutput,latestDealType,latestDealPrice)
            
        
            if 100*(totalInput - totalOutput) > biggestCashOccupy:
                biggestCashOccupy = 100*(totalInput - totalOutput)
            
        elif avgPriceToday >= 1.1*holdAvgPrice and avgPriceToday >= latestDealPrice*1.1 and holdShares > 0 :
            #价格大于上次交易价格1.1倍，且大于平均持仓成本的1.1倍，卖出持仓数/2上取整手
            sellShare = math.ceil(holdShares/2)

            if holdShares > sellShare:
                #尚未全部卖出，只是部分卖出
                holdAvgPrice = (holdShares*holdAvgPrice - sellShare*avgPriceToday)/(holdShares-sellShare)
                holdShares -= sellShare
            else:
                #如果全部卖出，则把平均持仓价格设置为0
                holdAvgPrice = 0
                holdShares = 0
                
            latestDealType = -1
            latestDealPrice = avgPriceToday
            totalOutput += sellShare*avgPriceToday
            
        
            if 100*(totalInput - totalOutput) > biggestCashOccupy:
                biggestCashOccupy = 100*(totalInput - totalOutput)
            
#            print(todayDate)
#            print('完成卖出交易，以%f价格卖出%i手股票'%(round(avgPriceToday,2),sellShare))
            printTradeInfo(todayDate, -1, avgPriceToday,holdShares,holdAvgPrice,totalInput,totalOutput,latestDealType,latestDealPrice)
        else:
            #没有任何交易，打印对账信息:
            printTradeInfo(todayDate, 0, avgPriceToday,holdShares,holdAvgPrice,totalInput,totalOutput,latestDealType,latestDealPrice)
            
    i+=1

'''
    #输出当前最新一天的盈亏情况
    if i==stock_his.shape[0]:
#        print('最新盈亏情况: ')
#        print(todayDate)
        printTradeInfo(todayDate, 0, avgPriceToday,holdShares,totalInput,totalOutput,latestDealType,latestDealPrice)
'''


print('最大资金占用金额: ', round(biggestCashOccupy,2))

sys.stdout = savedStdout  #恢复标准输出流


'''
规则：
1、以第一天中间价买入n手，如果后续某天中间价大于持仓成本的1.1倍则抛出一半
2、价格跌破最近一次交易价格的0.85时，再次买入n/2下取整手
3.如果持仓不为0，价格涨破平均持仓平均成本的1.1，且价格涨破上笔交易的1.1倍时，卖出持仓数/2上取整

'''




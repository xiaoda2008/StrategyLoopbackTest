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
from pathlib import Path
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import STARTDATE,ENDDATE,OUTPUTDIR

import time
import sqlalchemy
import os
import sys
from cmath import isnan
from sqlalchemy.util.langhelpers import NoneType
from pandas.core.frame import DataFrame
from com.xiaoda.stock.loopbacktester.utils.FileUtils import FileProcessor



import datetime
from scipy import optimize
from Tools.scripts.nm2def import NM
from test.test_decimal import cfractions
 

tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')

sdDataAPI = tushare.pro_api()


def getlastquarterfirstday():
    today=dt.now()
    quarter = (today.month-1)/3+1
    if quarter == 1:
        return dt(today.year-1,10,1)
    elif quarter == 2:
        return dt(today.year,1,1)
    elif quarter == 3:
        return dt(today.year,4,1)
    else:
        return dt(today.year,7,1)

startday=getlastquarterfirstday().strftime('%Y%m%d')


sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')


#stockTuples={}

cfRatioDict={}

cnt=0
for idx in sdf.index:
#    stockTuples[sdf.at[idx,'ts_code']]=sdf.at[idx,'name']

    if cnt>120:
        break
    else:
        cnt=cnt+1
    #获取资产负债表，总资产
    bs = sdDataAPI.balancesheet(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'), fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,cap_rese,total_assets')
    bs.at[0,'total_assets']
    time.sleep(0.75)
    
    #获取现金流量表中，现金等价物总数
    cf = sdDataAPI.cashflow(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'))#, period='20190930')
    cf.at[0,'c_cash_equ_end_period']

    ratio=cf.at[0,'c_cash_equ_end_period']/bs.at[0,'total_assets']
    
    cfRatioDict[sdf.at[idx,'ts_code']]=ratio

print()

sortedCFRatioList=sorted(cfRatioDict.items(),key=lambda x:x[1],reverse=True)


for cd, ratio in sortedCFRatioList:
    print(cd,' : ',round(ratio*100,2),'%')


topCFRatio100 = sortedCFRatioList[:100]

#gpr:毛利率
#npr:净利率


df= tushare.get_stock_basics()

#df=df.set_index('name')


idx=df.name.index
val=df.name.values

print(idx,val)

for cd in idx:
    
    print(cd, df.at[cd,'name'])
'''
    if 'ST' in df.at[cd,'name']:
        print('ST,%s,%s'%(cd,df.at[cd,'name']))
        df.drop(cd,inplace=True)
    elif '退' in df.at[cd,'name']:
        print('退,%s,%s'%(cd,df.at[cd,'name']))
        df.drop(cd,inplace=True)
    elif cd=='002680':
        print()
'''        

print()


'''
i=0

for nm in stNames:
    if 'ST' in nm:
        print('ST')

    i=i+1
'''

'''
df=df[~df.name.str.contains('ST')]

print(df)
'''

#获取不复权的数据
stock_k_data = tushare.pro_bar(ts_code='000001.SZ',start_date='20000101')

stock_k_data.to_csv('D:/outputDir/000001.csv',mode='wt')
df = sdDataAPI.adj_factor(ts_code='000001.SZ',trade_date='')

df.to_csv('D:/outputDir/000001-adj.csv',mode='wt')
#获取不复权的数据

stock_k_data = tushare.pro_bar(ts_code='000001.SZ',adj='qfq',start_date='20000101')

stock_k_data.to_csv('D:/outputDir/000001-qfq.csv',mode='wt')

#1、获取交易日信息，并存入数据库
STARTDATE = '20050101'#20071016
ENDDATE = '20050624'#20081031


#不能这样处理，不同区间取到的前复权数据不同，会影像处理的准确性
stock_k_data = tushare.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20000101')

#sdDataAPI.query('trade_cal', start_date='20180101', end_date='20181231')

trade_cal_data = sdDataAPI.trade_cal(exchange='', start_date=STARTDATE, end_date=ENDDATE)



# 函数
def xnpv(rate, cashflows):
    return sum([cf/(1+rate)**((t-cashflows[0][0]).days/365.0) for (t,cf) in cashflows])
 
def xirr(cashflows, guess=0.1):
    try:
        return optimize.newton(lambda r: xnpv(r,cashflows),guess)
    except:
        print('Calc Wrong')
 
 
# 测试
data = [(datetime.date(2006, 1, 1), -10000), (datetime.date(2007, 1, 1), 20000)]
print(xirr(data))










#记录csv内容的列表
fileContentTupleList = []

strOutterOutputDir=OUTPUTDIR+'/'+STARTDATE+'-'+ENDDATE+'/SMAStrategy/'

#读取文件列表
fileList = os.listdir(strOutterOutputDir)

#对文件列表中的文件进行处理，获取内容列表
for fileStr in fileList:
    if not fileStr=="Summary.csv":
        df = FileProcessor.readFile(strOutterOutputDir+fileStr)
        fileContentTupleList.append((fileStr[:-4],df))



#对各个日期计算相应的资金净流量
cashFlowTuple= {}
#对已有的内容列表进行处理
for fileName,fileDF in fileContentTupleList:
    print(fileName)

    #如果Summary-all.csv已经存在，则直接覆盖
  
    i=0
    while True:
        if not (fileDF.at[i,'日期'] in cashFlowTuple):
            cashFlowTuple[fileDF.at[i,'日期']] = float(fileDF.at[i,'当天资金净流量'])
        else:
            cashFlowTuple[fileDF.at[i,'日期']] = float(cashFlowTuple[fileDF.at[i,'日期']])+float(fileDF.at[i,'当天资金净流量'])
        i=i+1
        if i==len(fileDF):
            break
    

savedStdout = sys.stdout  #保存标准输出流
sys.stdout = open(OUTPUTDIR+'/'+STARTDATE+'-'+ENDDATE+'/Summary-all.csv','wt+')

print('日期,当日资金净流量')
for key in cashFlowTuple.keys():
    print(key,end=',')
    print(cashFlowTuple.get(key))

sys.stdout = savedStdout  #恢复标准输出流








tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')

sdDataAPI = tushare.pro_api()

#1、获取交易日信息，并存入数据库
STARTDATE = '20050101'#20071016
ENDDATE = '20050624'#20081031

trade_cal_data = sdDataAPI.trade_cal(exchange='', start_date=STARTDATE, end_date=ENDDATE)

#不能这样处理，不同区间取到的前复权数据不同，会影像处理的准确性
stock_k_data = tushare.pro_bar(ts_code='000001.SZ', adj='qfq', start_date=STARTDATE, end_date=ENDDATE)

stock_k_data=stock_k_data.set_index('trade_date')

stock_k_data[(stock_k_data.index=='20000622')]

if (stock_k_data[(stock_k_data.index=='20000622')]).empty:
    print("empty")

#写入数据库的引擎
engine = sqlalchemy.create_engine('mysql+pymysql://root:xiaoda001@localhost/tsdata?charset=utf8')


k_data = DataFrame()

k_data.sort_index(inplace=True,ascending=False)

k_data.to_sql(name='k_data_test', con=engine, chunksize=1000, if_exists='replace', index=None)



tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')

sdDataAPI = tushare.pro_api()

sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

stockCodeList = sdf['ts_code']


for index,stockCode in stockCodeList.items():

    print(index, stockCode)

#取000001的前复权行情
stock_k_data = tushare.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20091031', end_date='20191031')


if type(stock_k_data)==NoneType:
    print("empty")
    
stock_k_data.sort_index(inplace=True,ascending=False)

stock_k_data.reset_index(drop=True,inplace=True)


stock_k_data['MA20'] = stock_k_data['close'].rolling(20).mean()

offset = stock_k_data.index[0]
#剔除掉向前找的20个交易日数据
if stock_k_data.shape[0] > 20:
    stock_k_data = stock_k_data.drop([offset,offset+1])




data = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')


stockCodeList = data['ts_code']
#000948在2019.4.12等日期的复权数据有问题

stock_k_data = tushare.get_k_data('000948',start='2019-04-02',end='2019-04-15')

stock_k_data2 = stock_k_data.set_index('date',inplace=False)



#股票代码库
INPUTFILE = 'D:/stockList.txt'
STARTDATE = '1990-12-19'#2007-10-16
ENDDATE = '2019-10-31'#2008-10-31

#写入数据库的引擎
engine = sqlalchemy.create_engine('mysql+pymysql://root:xiaoda001@localhost/tsdata?charset=utf8')

#对所有股票代码，循环进行处理
in_text = open(INPUTFILE, 'r')

#循环所有股票，获取数据并写入数据库
for line in in_text.readlines():
    stockCode = line.rstrip("\n")
    df = tushare.get_k_data(code=stockCode,start=STARTDATE, end=ENDDATE)
    #存入数据库
    df.to_sql(name='k_data_'+stockCode, con=engine, chunksize=1000, if_exists='replace', index=None)






print(os.getcwd())
path = sys.path[0]
print(os.listdir())


#需要找到开始日期前面的20个交易日那天，从那一天开始获取数据
firstOpenDay='2014-05-19'
'''
cday = dt.strptime(firstOpenDay, "%Y-%m-%d").date()
dayOffset = datetime.timedelta(1)
cnt=0
# 获取想要的日期的时间
while True:
    cday = (cday - dayOffset)
    if not(tushare.is_holiday(cday.strftime('%Y-%m-%d'))):
        cnt+=1
        if cnt==20:
            break
firstOpenDay=cday.strftime('%Y-%m-%d')
'''
stock_k_data = tushare.get_k_data('000001',start=firstOpenDay,end='2015-06-12')

stock_k_data2 = stock_k_data.set_index('date',inplace=False)

print(stock_k_data2.at['2014-05-19','open'])

stock_k_data2.reset_index()


stock_k_data['SMA_20'] = stock_k_data['close'].rolling(20).mean()

offset = stock_k_data.index[0]
stock_k_data = stock_k_data.drop([offset,offset+1,offset+2,offset+3,offset+4,offset+5,offset+6,offset+7, \
                                  offset+8,offset+9,offset+10,offset+11,offset+12,offset+13,offset+14, \
                                  offset+15,offset+16,offset+17,offset+18,offset+19])

print()







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








stock_his = tushare.get_k_data('601366',start='2018-01-01',end='')
stock_his.shape[0]
stock_hist_data = tushare.get_hist_data(code='601366', start='2018-01-01')

stock_hist_data = stock_hist_data.sort_index()
stock_hist_data.shape[0]
todayMA20 = stock_hist_data.at['2018-01-03','ma20']


stock_his.set_index('date',inplace=True)


stock_his['SMA_20'] = stock_his['close'].rolling(20).mean()


stock_his['close_shift'] = stock_his['close'].shift(1)

#stock_his.plot(subplots=True,figsize=(10,6))

#stock_his[['close','SMA_20']].plot(figsize=(10,6))

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




'''
Created on 2019年12月16日

@author: xiaoda
'''
import os
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
from datetime  import datetime as dt
from  com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger

import multiprocessing

from multiprocessing import Manager

log=Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
 
 

def get8DictListFromStockDict(allStockDict):
    stDictList=[]
    dfLen=len(allStockDict)
    #对所有股票进行遍历
    i=0
    tmpDict1={}
    tmpDict2={}
    tmpDict3={}
    tmpDict4={}
    tmpDict5={}
    tmpDict6={}
    tmpDict7={}
    tmpDict8={}
    
    for (stockCode,scdict) in allStockDict.items():
        if i<dfLen*1/8:
            tmpDict1[stockCode]=scdict
        elif i>=dfLen*1/8 and i<dfLen*2/8:
            tmpDict2[stockCode]=scdict
        elif i>=dfLen*2/8 and i<dfLen*3/8:
            tmpDict3[stockCode]=scdict
        elif i>=dfLen*3/8 and i<dfLen*4/8:
            tmpDict4[stockCode]=scdict
        elif i>=dfLen*4/8 and i<dfLen*5/8:
            tmpDict5[stockCode]=scdict
        elif i>=dfLen*5/8 and i<dfLen*6/8:
            tmpDict6[stockCode]=scdict
        elif i>=dfLen*6/8 and i<dfLen*7/8:
            tmpDict7[stockCode]=scdict
        elif i>=dfLen*7/8 and i<dfLen*8/8:
            tmpDict8[stockCode]=scdict            
        i=i+1
    stDictList.append(tmpDict1)
    stDictList.append(tmpDict2)
    stDictList.append(tmpDict3)
    stDictList.append(tmpDict4)
    stDictList.append(tmpDict5)
    stDictList.append(tmpDict6)
    stDictList.append(tmpDict7)
    stDictList.append(tmpDict8)
                
    return stDictList


def testFun(i):
    print("test Func%s"%(i))
    print(os.getpid())
    return os.getpid()

mysqlProcessor=MysqlProcessor()
sdProcessor=StockDataProcessor()


def getVolatilityDFForIndustries(startday,endday):

    
    
    allStockDict=sdProcessor.getAllStockDataDict()
    #将需要处理的股票分成8份，以便分配到8个cpu上去处理
    
    stDictList=get8DictListFromStockDict(allStockDict)
    
    industryVolDict={}
    
    manager = Manager()
    #return_list = manager.list() #也可以使用列表list
    return_dict = manager.dict()
    
    #分8个进程，分别计算8段股票波动率
    
    process=[]


    i=0
    for stockDict in stDictList:
        stockList=list(stockDict.keys())
        i=i+1
        
        p=multiprocessing.Process(target=getVolatilityDFForIndustriesForStocks,args=(i,return_dict,stockList,startday,endday,))
        p.start()

        process.append(p)

    
    for p in process:
        p.join()
    

    retvalues=return_dict.values()
    for retVal in retvalues:
        
        for ind in retVal.keys():
            
            if not(ind in industryVolDict.keys()):
                #该产业尚未出现，则直接赋值即可
                industryVolDict[ind]=retVal[ind]

            else:
                #该产业已经存在，则需要取数后进行算术平均
                preNum=industryVolDict[ind]['num']
                preMaxRetRate=industryVolDict[ind]['val'][0]
                preMaxIncRate=industryVolDict[ind]['val'][1]    
                
                currNum=retVal[ind]['num']
                currMaxRetRate=retVal[ind]['val'][0]
                currMaxIncRate=retVal[ind]['val'][1]

                num=currNum+preNum
                maxRRate=(preMaxRetRate*preNum+currMaxRetRate*currNum)/num
                maxIRate=(preMaxIncRate*preNum+currMaxIncRate*currNum)/num
                
                industryVolDict[ind]={'num':num,'val':(maxRRate,maxIRate)}
        
    print(len(industryVolDict))
    #pool.join()
    
    #将8个进程计算出来的均值进行合并
    
    
    return industryVolDict
    #for stockDict in stDictList:
    #    industryVolDict=self.getVolatilityDFForIndustriesForStocks(stockDict,startday,last_endday)
    #    industryVolDictList.append(industryVolDict)
    



def getVolatilityDFForIndustriesForStocks(i,return_dict,stockList,startday,endday):


    print("当前进程ID:%s"%(os.getpid()))
    #用于记录每个行业的波动率的字典
    industryVolDict={}
    
    #对所有股票进行遍历
    for stockCode in stockList:
        
        log.logger.info('ProcessID:%s,处理%s'%(os.getpid(),stockCode))
        
        scdict=sdProcessor.getStockInfo(stockCode)
        
        list_date=scdict.at[0,'list_date']
        #对于上市时间比计算时间段短的股票进行剔除
        if list_date>startday:
            continue
        
        industry=scdict.at[0,'industry']

        if industry==None:
            continue
        if not (industry in industryVolDict):
            #还未处理过该行业类别的股票
            valForStock=calVolatilityForStock(stockCode,startday,endday)
            industryVolDict[industry]={'num':1,'val':valForStock}

        else:
            #已经处理过，则取出原有数据
            #与当前股票数据取平均值后再次赋值
            valForStock=calVolatilityForStock(stockCode,startday,endday)
            
            num=industryVolDict[industry]['num']
            valForInd=industryVolDict[industry]['val']
            
            
            avgVal=(round((num*valForInd[0]+valForStock[0])/(num+1),4),round((num*valForInd[1]+valForStock[1])/(num+1),4))
            #avgVal=round((num*valForInd+valForStock)/(num+1),4)
            
            industryVolDict[industry]={'num':num+1,'val':avgVal}
    
    #对每个股票进行遍历，
    #计算其最近一年内的波动幅度，并将同一行业的股票波动幅度求平均值
    return_dict[i]=industryVolDict
    #return industryVolDict

 
def calVolatilityForStock(stockCode,startday,endday):
    #对于股票stockCode，计算过去一段时间的波动率
    #波动率用什么表示？
    #涨跌幅绝对值的平均值？
    #最大跌幅、最大涨幅？-》可能用最大涨跌幅更合理

    stock_k_data=sdProcessor.getStockKData(stockCode,startday,endday,'qfq')

    if stock_k_data.empty:
        return (0,0)
    
    stock_k_data.set_index('trade_date',drop=True, inplace=True)

    maxRetRate,maxIncRate=getMaxRetIncRateForStockInPeriod(stock_k_data,startday,endday)
    
    return (maxRetRate,maxIncRate)


def getMaxRetIncRateForStockInPeriod(stock_k_data,sd,ed):
    '''
    计算股票在最近一段时间内的最大回撤率
    一年内最大回撤率不一定等于最高减去最低，因为一方面有可能高点出现在后面
    另一方面，还有个比例问题，不一定绝对值最高最低就是最大回撤率或者上涨率
    对于每一个交易日，计算其后最大的回撤率，最大上涨率
    取所有交易日中最大的回撤率的最大值就是最大回撤率
    最大上涨率同理
    '''
    maxRetRate=0
    maxIncRate=0
    
    currday=sd
    
    #对stock_k_date进行按交易日扫描
    #对每个交易日，向后扫描此后所有交易日
    #如果高于当前价格，则取其上涨幅度与当前价格相比，计算最大上涨率
    #如果低于当前价格，则取其回撤幅度与当前价格相比，计算最大回撤率
    while currday<=ed:
        
        if not sdProcessor.isDealDay(currday):
            currday=sdProcessor.getNextCalDay(currday)
            continue
        
        try:
            highPriceToday=float(stock_k_data.at[currday,'high'])
            lowPriceToday=float(stock_k_data.at[currday,'low'])
        except KeyError:
            #说明当天该股票停牌
            currday=sdProcessor.getNextCalDay(currday)
            continue

        
        highestPrice=getHighestPriceInLastYearFromDate(stock_k_data,currday,ed)
        lowestPrice=getLowestPriceInLastYearFromDate(stock_k_data,currday,ed)
        
        if (lowestPrice/highPriceToday-1)<maxRetRate:
            maxRetRate=lowestPrice/highPriceToday-1
        
        if (highestPrice/lowPriceToday-1)>maxIncRate:
            maxIncRate=highestPrice/lowPriceToday-1

        #获取下一自然日
        currday=sdProcessor.getNextCalDay(currday)
        
    return maxRetRate,maxIncRate




def getHighestPriceInLastYearFromDate(stock_k_data,sd,ed):
    '''计算股票在最近一段时间内从某一时间点往后最高价格'''
    
    highestPrice=0
    currday=sd
    
    while currday<=ed:
        
        if not sdProcessor.isDealDay(currday):
            currday=sdProcessor.getNextCalDay(currday)
            continue

        try:
            highPriceToday=float(stock_k_data.at[currday,'high'])
        except KeyError:
            #说明当天该股票停牌
            currday=sdProcessor.getNextCalDay(currday)
            continue
        
        if highPriceToday>highestPrice:
            highestPrice=highPriceToday

        #获取下一自然日
        currday=sdProcessor.getNextCalDay(currday)     
       
    return highestPrice



def getLowestPriceInLastYearFromDate(stock_k_data,sd,ed):
    '''计算股票在最近一段时间内最低价格'''
    
    lowestPrice=99999
    currday=sd
    
    while currday<=ed:
        
        if not sdProcessor.isDealDay(currday):
            currday=sdProcessor.getNextCalDay(currday)
            continue

        try:
            lowPriceToday=float(stock_k_data.at[currday,'low'])
        except KeyError:
            #说明当天该股票停牌
            currday=sdProcessor.getNextCalDay(currday)
            continue
        
        
        if lowPriceToday<lowestPrice:
            lowestPrice=lowPriceToday

        #获取下一自然日
        currday=sdProcessor.getNextCalDay(currday)     
       
    return lowestPrice
    
if __name__ == "__main__":
    
    endday='20161231'
    
    sql = "select content from u_data_desc where content_name='data_end_dealday'"
    res=mysqlProcessor.querySql(sql)
    endDealDay=res.at[0,'content']
    
    if endDealDay<endday:
        endday=endDealDay
    
    startday=StockDataProcessor.getCalDayByOffset(endday,-365)
    
    volDict=getVolatilityDFForIndustries(startday,endday)
    
    
    #把数据持久化到数据库中去？
    
    
    indList=list(volDict.keys())
    for ind in indList:
        
        num=volDict.get(ind)['num']
        maxRetRt=volDict.get(ind)['val'][0]
        maxIncRt=volDict.get(ind)['val'][1]
        
        mysqlProcessor=MysqlProcessor()
        mysqlSession=mysqlProcessor.getMysqlSession()
        sql='replace into u_vol_for_industry values(\'%s\',%d,%f,%f,\'%s\',\'%s\')'%(ind,num,maxRetRt,maxIncRt,startday,endday)
        mysqlProcessor.execSql(mysqlSession, sql, True)
    
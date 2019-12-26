'''
Created on 2019年12月20日

@author: xiaoda
'''

import math
import os
from com.xiaoda.stock.loopbacktester.strategy.trade.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import nShare,RetRate,IncRate
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
import pandas as pd
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
from com.xiaoda.stock.loopbacktester.utils.TradeChargeUtils import TradeChargeProcessor

class BLSHPlusMAStrategy(StrategyParent):
    '''规则：
    #1、以第一天中间价买入n手
    2、价格跌破最近一次交易价格的(1-DOWNRATE)时，再次买入n/2手
    3.如果持仓不为0，价格涨破平均持仓平均成本的(1+UPRATE)，且价格涨破上笔交易的(1+UPRATE)倍时，卖出持仓数/2
    '''

    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="BLSHPlusMAStrategy"
        self.mysqlProcessor=MysqlProcessor()
        sql='select * from u_vol_for_industry'
        self.volForIndDF=self.mysqlProcessor.querySql(sql)
        self.volForIndDF.set_index('industry',drop=True,inplace=True)
        self.sdProcessor=StockDataProcessor()
       
    def getStockTradeDF(self,currday,dealType,closePriceToday,holdShares,holdAvgPrice,netCashFlowToday,
                   totalInput,totalOutput,latestDealType,latestDealPrice,dealCharge):
        
        #t11=timer()
        #print("t11:%s"%(t11))
        currentProfit=round(closePriceToday*holdShares*100+totalOutput-totalInput,4)
        if totalInput>0:
            totalProfitRate=currentProfit/totalInput*100
        else:
            totalProfitRate=0
        
        
        #new=pd.Series({"lib":1,"qty1":2,'qty2':3},name="a")   # 自定义索引为：1 ，这里也可以不设置index
        #new=pd.DataFrame({'lib':'a','qty1':'b','qty2':'c'},index=[1])   # 自定义索引为：1 ，这里也可以不设置index
        #stockOutDF=stockOutDF.append(new,ignore_index=True,sort=False)   # ignore_index=True,表示不按原来的索引，从0开始自动递增
        
          
        returnDF=pd.DataFrame({'日期':currday,\
                               '交易类型':dealType,\
                               '当天收盘持仓市值':round(closePriceToday*holdShares*100,4),\
                               '当天持仓手数':holdShares,\
                               '累计投入金额':round(totalInput,4),\
                               '累计赎回金额':round(totalOutput,4),\
                               '当天资金净占用':round(totalInput-totalOutput,4),\
                               '当天资金净流量':round(netCashFlowToday,4),\
                               '当前持仓平均成本':round(holdAvgPrice,4),\
                               '当天收盘价格':round(closePriceToday,4),\
                               '当前持仓盈亏':currentProfit,\
                               '最近一次交易类型':latestDealType,\
                               '最近一次交易价格':round(latestDealPrice,4),\
                               '当前全部投入回报率':"%.2f%%"%(totalProfitRate),\
                               '当天交易手续费':round(dealCharge,2),\
                               '当天是否交易(可能非交易日或者停盘)':1},index=[1])
        '''                                   
        returnDF=pd.DataFrame([[currday,\
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
        '''
        
        #t12=timer()
        #print("t12:%s"%(t12))
        #print("t12-t11:%s"%(t12-t11))
        return returnDF
     
    #对于非交易日，需要将上一交易日数据重新输出一行
    def getDupLastDayDF(self,currday,lastDealDayDF):
        
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
        
        return lastDealDayDF

     
    def processTradeDuringPeriod(self,stockCode,startday,endday):
        '''
        对stockCode股票从startday到endday之间的交易进行处理
        并返回一个结构，用于表示实际的每天的交易及每天的持仓信息
        '''
        #tc=timer()
        #print("tc:%s"%(tc))
        stockOutDF=pd.DataFrame(columns=('日期','交易类型','当天收盘持仓市值','当天持仓手数','累计投入金额',\
                                         '累计赎回金额','当天资金净占用','当天资金净流量','当前持仓平均成本',\
                                         '当天收盘价格','当前持仓盈亏','最近一次交易类型','最近一次交易价格',\
                                         '当前全部投入回报率','当天交易手续费','当天是否交易(可能非交易日或者停盘)'))

        #获取第一个交易日
        firstDealDay=self.sdProcessor.getNextDealDay(startday,True)
        
        stock_k_data=self.sdProcessor.getStockKData(stockCode,firstDealDay,endday,'qfq')

        #如果需要用到MA等的数据，需要提前预处理一下Kdata
        #stock_k_data=self.sdProcessor.preProcessKDataDF(stock_k_data)

        #如果需要用到所属行业信息，需要提前获取
        #sql='select industry from u_stock_list where ts_code=\'%s\''%(stockCode)
        #idf=self.mysqlProcessor.querySql(sql)
        #stockInd=idf.at[0,'industry']
              
        if stock_k_data.empty:
            self.log.logger.info('%s为新上市股票，或存在停牌情况，进行剔除'%(stockCode))
            return pd.DataFrame()
        
                

        stock_k_data.reset_index(drop=True,inplace=True)
    
        #计算出MA20的数据，问题在于，这个MA20是包含当天的，有些问题，应当不包含当天
        stock_k_data['pre_MA20'] = stock_k_data['pre_close'].rolling(20).mean()
        stock_k_data['pre_MA10'] = stock_k_data['pre_close'].rolling(10).mean()
        stock_k_data['pre_MA5'] = stock_k_data['pre_close'].rolling(5).mean()
                        
        #获得前一天的最高值和最低值
        stock_k_data['pre_high'] = stock_k_data['high'].shift(1)
        stock_k_data['pre_low'] = stock_k_data['low'].shift(1)
         
        
        offset=stock_k_data.index[0]
        
        #剔除掉向前找的20个交易日数据
        #不要这样剔除，而是一直剔除到firstOpenDay
    
    
        currday=firstDealDay
        
        a=0
        flg=False
        while True:
            
            #删空了，在向前推进的交易日有交易，但在查询区间内股票就没有交易
            if stock_k_data.empty:
                break
            
            #允许正好这一天股票停牌
            if stock_k_data.at[a+offset,'trade_date']==firstDealDay:
                break
            elif stock_k_data.at[a+offset,'trade_date']>firstDealDay:
                #直接大于，说明第一天该股票就停牌，进行特殊处理
                flg=True
                break
            else:
                stock_k_data=stock_k_data.drop([a+offset])
                a=a+1
                
        if flg==True:
            ##直接大于，说明第一天该股票就停牌，进行特殊处理
            currday=stock_k_data.at[a+offset,'trade_date']

        
        stock_k_data.set_index('trade_date',drop=True, inplace=True)



        holdShares=0#持仓手数，1手为100股
        holdAvgPrice=0#持仓的平均价格
        latestDealType=0#最近一笔成交类型，-1表示卖出，1表示买入
        latestDealPrice=0#最近一笔成交的价格
        totalInput=0#累计总投入
        totalOutput=0#累计总赎回
        biggestCashOccupy=0#最大占用总金额，为totalInput-totalOutput的最大值       
        #最大连续上涨/下跌买入/卖出计数：连续上涨买入为正数，连续下跌卖出为负数，连续上涨超线买入，计数器加1，连续下跌超线卖出，计数器减1
        continuousRiseOrFallCnt=0

        
        closePriceToday=stock_k_data.at[currday,'close']
    
        #openPrice=float(stock_k_data.at[currday,'open'])
        #closePrice=float(stock_k_data.at[currday,'close'])
        highPrice=float(stock_k_data.at[currday,'high'])
        lowPrice=float(stock_k_data.at[currday,'low'])
        avgPrice=(highPrice+lowPrice)/2
 
        #Simple策略第一天，直接以当日平均价格进行买入
        sharesToBuyOrSell=nShare
        priceToBuyOrSell=avgPrice
    
        if sharesToBuyOrSell>0:
            #如果判断为应当买入
            #更新持仓平均成本
            holdAvgPrice=(holdShares*holdAvgPrice+sharesToBuyOrSell*priceToBuyOrSell)/(holdShares+sharesToBuyOrSell)
            holdShares+=sharesToBuyOrSell
            
            #获取买入交易费
            dealCharge=TradeChargeProcessor.getBuyCharge(sharesToBuyOrSell*100*priceToBuyOrSell)
            
            latestDealType=1
            latestDealPrice=priceToBuyOrSell
            totalInput+=sharesToBuyOrSell*priceToBuyOrSell*100+dealCharge
            netCashFlowToday=-(sharesToBuyOrSell*priceToBuyOrSell*100+dealCharge)
             
            appendlDF=self.getStockTradeDF(currday,1,closePriceToday,holdShares,
                                        holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                        latestDealType,latestDealPrice,dealCharge)
            
            stockOutDF=stockOutDF.append(appendlDF,ignore_index=True,sort=False)
            
            biggestCashOccupy=totalInput
        else:
            #第一天不可能判断为卖出没有意义，没有份额可以卖出
            #既不需要买入，又不需要卖出
            #没有任何交易，打印对账信息:
    
            netCashFlowToday=0
             
            appendlDF=self.getStockTradeDF(currday,0,closePriceToday,holdShares,
                            holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                            latestDealType,latestDealPrice,0)

            stockOutDF=stockOutDF.append(appendlDF,ignore_index=True,sort=False)

        #获取下一自然日
        currday=self.sdProcessor.getNextCalDay(currday)
        
        #用于传递最近一个交易日的数据
        #用于在非交易日继续重复输出
        lastDealDayDF=appendlDF
        
        #td=timer()
        #print("td:%s"%(td))
        #print("td-tc:%s"%(td-tc))
        
        
        while currday<=endday:
            #ta=timer()
            #print("ta:%s"%(ta))
            if (not self.sdProcessor.isDealDay(currday)):
                #当前日期非交易日，需要输出一个空行到文件中
                #该空行内容与上一个交易日的内容相同
                appendlDF=self.getDupLastDayDF(currday,lastDealDayDF)

                stockOutDF=stockOutDF.append(appendlDF,ignore_index=True,sort=False)
                
                currday=StockDataProcessor.getNextCalDay(currday)
                continue
    
            try:
                closePriceToday=stock_k_data.at[currday,'close']
            except KeyError:
                #该交易日股票停牌
                #需要输出一个空行到文件中
                #该空行内容与上一个交易日的内容相同
    
                appendlDF=self.getDupLastDayDF(currday,lastDealDayDF)

                stockOutDF.append(appendlDF,ignore_index=True,sort=False)

                '''                            
                #如果直接停牌到endday
                #这样处理会导致无法输出数据到Summary文件中
                if currday==lastDealDay:
                    #最后一个交易日的盈利情况
                    latestProfit=lastDealDayDF.iat[0,10]
                    
                    #最后一个交易日这里，并行计算是否会出现问题?
                      
                    printSummaryTradeInfo(summaryOutFile,stockCode,biggestCashOccupy,totalInput,totalOutput,
                                    latestProfit,holdShares,closePriceToday)
                
                '''
                currday=StockDataProcessor.getNextCalDay(currday)
                continue
            
            #openPrice=float(stock_k_data.at[currday,'open'])
            #closePrice=float(stock_k_data.at[currday,'close'])
            highPrice=float(stock_k_data.at[currday,'high'])
            lowPrice=float(stock_k_data.at[currday,'low'])
            avgPrice=(highPrice+lowPrice)/2
            
                
            #todayMA20 = float(stock_k_data.at[currday,'today_MA20'])
            pre_MA20=float(stock_k_data.at[currday,'pre_MA20'])
            pre_MA10=float(stock_k_data.at[currday,'pre_MA10'])
            pre_MA5=float(stock_k_data.at[currday,'pre_MA5'])
            
            #pre_high=float(stock_k_data.at[currday,'pre_high'])
            #pre_low=float(stock_k_data.at[currday,'pre_low'])
            #pre_close=float(stock_k_data.at[currday,'pre_close'])
            
            
            
            #如果没有MA20数据
            if pd.isna(pre_MA20) or pd.isna(pre_MA10) or pd.isna(pre_MA5):
                sharesToBuyOrSell=0
                priceToBuyOrSell=0
    
            
            if lowPrice<(1+RetRate)*latestDealPrice and lowPrice>pre_MA5:
                #如果下跌超线，应当买入
                sharesToBuyOrSell=math.floor(nShare/2)
                priceToBuyOrSell=(1+RetRate)*latestDealPrice
                
            elif highPrice>(1+IncRate)*holdAvgPrice and highPrice>latestDealPrice*(1+IncRate)\
            and holdShares>0 and highPrice<pre_MA10:
                #如果上涨超线，应当卖出
                sharesToBuyOrSell=-1*math.ceil(holdShares/2)
                priceToBuyOrSell=max((1+IncRate)*holdAvgPrice,latestDealPrice*(1+IncRate))
            else:
                #未上涨或下跌超线
                sharesToBuyOrSell=0
                priceToBuyOrSell=0
            
            
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
                
                appendlDF=self.getStockTradeDF(currday,1,closePriceToday,holdShares,
                                            holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,dealCharge)                
                
                stockOutDF=stockOutDF.append(appendlDF,ignore_index=True,sort=False)

                
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
                dealCharge=TradeChargeProcessor.getSellCharge(abs(sharesToBuyOrSell)*100*priceToBuyOrSell)
                    
                latestDealType = -1
                latestDealPrice = priceToBuyOrSell
                totalOutput += abs(sharesToBuyOrSell)*priceToBuyOrSell*100-dealCharge
                netCashFlowToday=abs(sharesToBuyOrSell)*priceToBuyOrSell*100-dealCharge
            
                if totalInput - totalOutput > biggestCashOccupy:
                    biggestCashOccupy = totalInput - totalOutput
                
                appendlDF=self.getStockTradeDF(currday,-1,closePriceToday,holdShares,
                                            holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,dealCharge)                
                
                stockOutDF=stockOutDF.append(appendlDF,ignore_index=True,sort=False)
            
            else:
                #既不需要买入，又不需要卖出
                #没有任何交易，打印对账信息:
                netCashFlowToday=0
                
                appendlDF=self.getStockTradeDF(currday,0,closePriceToday,holdShares,
                                            holdAvgPrice,netCashFlowToday,totalInput,totalOutput,
                                            latestDealType,latestDealPrice,0)                
                
                stockOutDF=stockOutDF.append(appendlDF,ignore_index=True,sort=False)

            lastDealDayDF=appendlDF
        
            #new=pd.Series({"lib":1,"qty1":2,'qty2':3},name="a")   # 自定义索引为：1 ，这里也可以不设置index
            #new=pd.DataFrame({'lib':'a','qty1':'b','qty2':'c'},index=[1])   # 自定义索引为：1 ，这里也可以不设置index
            #stockOutDF=stockOutDF.append(new,ignore_index=True,sort=False)   # ignore_index=True,表示不按原来的索引，从0开始自动递增        
            
            currday=StockDataProcessor.getNextCalDay(currday)        
        
            #tb=timer()
            #print("tb:%s"%(tb))
            #print("tb-ta:%s"%(tb-ta))
            
        return stockOutDF
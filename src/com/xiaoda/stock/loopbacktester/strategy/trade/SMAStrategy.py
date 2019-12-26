'''
Created on 2019年10月29日

@author: picc
'''
import pandas
import math
import os
from com.xiaoda.stock.loopbacktester.strategy.trade.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import nShare
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger

import math
from com.xiaoda.stock.loopbacktester.strategy.trade.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import nShare,RetRate,IncRate
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
import pandas as pd
from com.xiaoda.stock.loopbacktester.utils.TradeChargeUtils import TradeChargeProcessor
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger



class SMAStrategy(StrategyParent):
    '''规则：
    #1、以第一天中间价买入n手
    
    2、价格跌破最近一次交易价格的(1-DOWNRATE)时，买入一定数量：
        （1）如果下跌计数continuousFallCnt=0，则只买入nShare/6的数量
        （2）如果下跌计数continuousFallCnt=1，则只买入nShare/3的数量
        （3）如果下跌计数continuousFallCnt>=2，则买入nShare/2的数量
    
    3.如果持仓不为0，价格涨破平均持仓平均成本的(1+UPRATE)，且价格涨破上笔交易的(1+UPRATE)倍时，卖出一定数量：
        （1） 如果上涨计数continuousRiseCnt=0，则只卖出持仓数/3的数量
        （2）如果上涨计数continuousRiseCnt=1，则只卖出持仓数/2的数量
        （3）如果上涨计数continuousRiseCnt=2，则卖出全部持仓数
    '''
    
    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="SMAStrategy"
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
        
        someDaysBeforeFirstOpenDay=self.sdProcessor.getDealDayByOffset(firstDealDay, -30)
        
        stock_k_data=self.sdProcessor.getStockKData(stockCode,someDaysBeforeFirstOpenDay,endday,'qfq')

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
    
        
        #用于传递最近一个交易日的数据
        #用于在非交易日继续重复输出
        lastDealDayDF=pd.DataFrame()
        
        
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
            
            openPrice=float(stock_k_data.at[currday,'open'])
            closePrice=float(stock_k_data.at[currday,'close'])
            highPrice=float(stock_k_data.at[currday,'high'])
            lowPrice=float(stock_k_data.at[currday,'low'])
            avgPrice=(highPrice+lowPrice)/2
            
            
            #todayMA20 = float(stock_k_data.at[currday,'today_MA20'])
            pre_MA20=float(stock_k_data.at[currday,'pre_MA20'])
            pre_high=float(stock_k_data.at[currday,'pre_high'])
            pre_low=float(stock_k_data.at[currday,'pre_low'])
            pre_close=float(stock_k_data.at[currday,'pre_close'])
            

        
            #如果没有MA20数据
            if pandas.isna(pre_MA20):
                sharesToBuyOrSell=0
                priceToBuyOrSell=0
            
            #需要调整，当天，只可能知道当天开盘价，无法知道当天平均价，不能采用上帝模式
    
            #应该用当天开盘价与前一天的MA20进行比较
            #在判断与20日均线的交叉时，应当统一应用同一标准
            #前一日采用收盘值则本日也要使用收盘值
            
            if pre_high<pre_MA20 and highPrice>pre_MA20:
                
                #上涨超过9%，暂时先按照最高价再涨5%买入进行计算
                if avgPrice>pre_close*1.09:
                    self.log.logger.info(stock_k_data.at[currday,'ts_code']+"在"+currday+"涨幅超过9%")
                    sharesToBuyOrSell=math.floor(nShare/2)
                    priceToBuyOrSell=highPrice*1.05
                else:
                    sharesToBuyOrSell=math.floor(nShare/2)
                    priceToBuyOrSell=pre_MA20
                #暂时还没有比较好的方法解决在前一日涨停、跌停，下一日买入的方法
                #因此，暂时先不考虑涨跌停不能买入/卖出的情况
                #if avgPrice<pre_close*1.09:
                #    #前日最高价格低于前日20日均线，且当日最高价突破前日20日均线-》上穿20日均线，可以买入
                #    #当天不是涨停状态超线，可以买入
                #    return math.floor(nShare/2)
                #else:
                #    return 0
            elif pre_low>pre_MA20 and lowPrice<pre_MA20:
                
                #如出现跌停，按照跌停价格再跌5%进行卖出计算
                if avgPrice<pre_close*0.91:
                    self.log.logger.info(stock_k_data.at[currday,'ts_code']+"在"+currday+"下跌超过9%")
                    sharesToBuyOrSell=-1*math.ceil(nShare/2)
                    priceToBuyOrSell=lowPrice*0.95
                else:
                    sharesToBuyOrSell=-1*math.ceil(nShare/2)
                    priceToBuyOrSell=pre_MA20
                #前一天收盘价格高于20日均线，且当天开盘价格低于20日均线-》下穿20日均线，可以卖出
                #当天不是以跌停状态超线
                #if avgPrice>float(stock_k_data.at[todayDate,'pre_close'])*0.9:
                #    return -1*math.ceil(nShare/2)
                #else:
                #    return 0
            else:
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

            currday=StockDataProcessor.getNextCalDay(currday)        
        
            #tb=timer()
            #print("tb:%s"%(tb))
            #print("tb-ta:%s"%(tb-ta))
            
        return stockOutDF


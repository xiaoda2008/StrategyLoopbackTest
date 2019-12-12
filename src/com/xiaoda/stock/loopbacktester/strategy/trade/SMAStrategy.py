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

class SMAStrategy(StrategyParent):
    '''
    SMA（简单移动平均）策略
            根据MA的数值变化情况，决定是否买入或者卖出
    '''
    
    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="SMAStrategy" 
 
#可参考的文章：https://www.jianshu.com/p/642ad8a0366e


    #决定买入或卖出的数量
    #正数代表买入，负数代表卖出
    #continuousRiseOrFallCnt，正数代表连续上涨，负数代表连续下跌
    #MA策略不考虑是否第一天，都按照同样的逻辑进行判断
    def getShareAndPriceToBuyOrSell(self,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseOrFallCnt,
                     stock_k_data,todayDate):
        
        #stock_k_data = stock_k_data.set_index('trade_date')
        
        #lastMarketDay=MysqlUtils.getLastMarketDay(todayDate)
        

        
        openPrice=float(stock_k_data.at[todayDate,'open'])
        closePrice=float(stock_k_data.at[todayDate,'close'])
        highPrice=float(stock_k_data.at[todayDate,'high'])
        lowPrice=float(stock_k_data.at[todayDate,'low'])
        avgPrice=(highPrice+lowPrice)/2
        
        
        #todayMA20 = float(stock_k_data.at[todayDate,'today_MA20'])
        pre_MA20=float(stock_k_data.at[todayDate,'pre_MA20'])
        pre_high=float(stock_k_data.at[todayDate,'pre_high'])
        pre_low=float(stock_k_data.at[todayDate,'pre_low'])
        pre_close=float(stock_k_data.at[todayDate,'pre_close'])
        
                       
        #如果没有MA20数据
        if pandas.isna(pre_MA20):
            return 0,0
        
        #需要调整，当天，只可能知道当天开盘价，无法知道当天平均价，不能采用上帝模式

        #应该用当天开盘价与前一天的MA20进行比较
        #在判断与20日均线的交叉时，应当统一应用同一标准
        #前一日采用收盘值则本日也要使用收盘值
        
        if pre_high<pre_MA20 and highPrice>pre_MA20:
            
            #上涨超过9%，暂时先按照最高价再涨5%买入进行计算
            if avgPrice>pre_close*1.09:
                self.log.logger.info(stock_k_data.at[todayDate,'ts_code']+"在"+todayDate+"涨幅超过9%")
                return math.floor(nShare/2), highPrice*1.05
            else:
                return math.floor(nShare/2), pre_MA20
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
                self.log.logger.info(stock_k_data.at[todayDate,'ts_code']+"在"+todayDate+"下跌超过9%")
                return -1*math.ceil(nShare/2), lowPrice*0.95
            else:
                return -1*math.ceil(nShare/2), pre_MA20
            #前一天收盘价格高于20日均线，且当天开盘价格低于20日均线-》下穿20日均线，可以卖出
            #当天不是以跌停状态超线
            #if avgPrice>float(stock_k_data.at[todayDate,'pre_close'])*0.9:
            #    return -1*math.ceil(nShare/2)
            #else:
            #    return 0
        else:
            return 0,0
        

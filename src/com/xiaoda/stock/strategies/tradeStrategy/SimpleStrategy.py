'''
Created on 2019年10月28日

@author: picc
'''
import math
from com.xiaoda.stock.strategies.tradeStrategy.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import *



class SimpleStrategy(StrategyParent):
    '''规则：
    #1、以第一天中间价买入n手
    2、价格跌破最近一次交易价格的(1-DOWNRATE)时，再次买入n/2手
    3.如果持仓不为0，价格涨破平均持仓平均成本的(1+UPRATE)，且价格涨破上笔交易的(1+UPRATE)倍时，卖出持仓数/2
    '''
    
   #决定买入或卖出的数量
    #正数代表买入，负数代表卖出
    #continuousRiseOrFallCnt，正数代表连续上涨，负数代表连续下跌

    def getShareToBuyOrSell(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseOrFallCnt,
                     stock_k_data,todayDate):
        #Simple策略第一天，直接进行买入
        if latestDealPrice==0 and latestDealType==0:
            return nShare
        
        if priceNow < (1-DOWNRATE)*latestDealPrice:
            #如果下跌超线，应当买入
            return math.floor(nShare/2)
        elif priceNow > (1+UPRATE)*holdAvgPrice and priceNow > latestDealPrice*(1+UPRATE) and holdShares>0:
            #如果上涨超线，应当卖出
            return -1*math.floor(holdShares/2)
        else:
            #未上涨或下跌超线
            return 0
 
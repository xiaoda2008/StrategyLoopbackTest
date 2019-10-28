'''
Created on 2019年10月28日

@author: picc
'''
import math
from com.xiaoda.stock.strategies.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import *



class Strategy1(StrategyParent):
    '''规则：
    #1、以第一天中间价买入n手
    2、价格跌破最近一次交易价格的(1-DOWNRATE)时，再次买入n/2手
    3.如果持仓不为0，价格涨破平均持仓平均成本的(1+UPRATE)，且价格涨破上笔交易的(1+UPRATE)倍时，卖出持仓数/2
    '''
    

    
#@staticmethod

    #决定应买入的数量
    def getShareToBuy(self, priceNow, latestDealPrice, 
                     latestDealType, holdShares,
                     holdAvgPrice, continuousFallCnt):
        
        if priceNow <= (1-DOWNRATE)*latestDealPrice:
            return math.floor(nShare/2)
        else:
            return 0
        
    
    #决定应当卖出的数量
    def getShareToSell(self, priceNow, latestDealPrice, 
                      latestDealType, holdShares,
                      holdAvgPrice, continuousRiseCnt):
        
        if priceNow >= (1+UPRATE)*holdAvgPrice and \
        priceNow >= latestDealPrice*(1+UPRATE) and holdShares > 0 :
            #价格大于上次交易价格(1+UPRATE)倍，且大于平均持仓成本的(1+UPRATE)倍，卖出持仓数/2上取整手
            return math.ceil(holdShares/2)
        else:
            return 0
    
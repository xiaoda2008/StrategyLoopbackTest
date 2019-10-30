'''
Created on 2019年10月30日

@author: picc
'''
import math
from com.xiaoda.stock.strategies.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import *

class SimplePlusSMAStrategy(StrategyParent):
    '''
    将基础的策略与SMA策略相结合
    在涨跌幅达到一定程度，且MA20满足一定条件的情况下才进行买卖
    可以考虑在股价下跌时，只有当股价开始向上穿越20日均线时再买入
    '''
    
    
    #决定买入或卖出的数量
    #正数代表买入，负数代表卖出
    #continuousRiseOrFallCnt，正数代表连续上涨，负数代表连续下跌
    def getShareToBuyOrSell(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseOrFallCnt,
                     stock_hist_data,todayDate):
        if priceNow <= (1-DOWNRATE)*latestDealPrice:
            #价格低于阈值，应当买入
            return math.floor(nShare/2)
        elif priceNow >= (1+UPRATE)*holdAvgPrice and \
        priceNow >= latestDealPrice*(1+UPRATE) and holdShares > 0 :
            #价格大于上次交易价格(1+UPRATE)倍，且大于平均持仓成本的(1+UPRATE)倍，卖出持仓数/2上取整手:
            return -1*math.ceil(holdShares/2)
        else:
            #不满足买入、卖出条件，不进行交易
            return 0
    
    '''
    #决定应买入的数量
    def getShareToBuy(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousFallCnt,
                     stock_hist_data,todayDate):
        
        if priceNow <= (1-DOWNRATE)*latestDealPrice:
            return math.floor(nShare/2)
        else:
            return 0
        
    '''
       
    '''
    #决定应当卖出的数量
    def getShareToSell(self,priceNow,latestDealPrice, 
                      latestDealType,holdShares,
                      holdAvgPrice,continuousRiseCnt,
                      stock_hist_data,todayDate):
        
        if priceNow >= (1+UPRATE)*holdAvgPrice and \
        priceNow >= latestDealPrice*(1+UPRATE) and holdShares > 0 :
            #价格大于上次交易价格(1+UPRATE)倍，且大于平均持仓成本的(1+UPRATE)倍，卖出持仓数/2上取整手
            return math.ceil(holdShares/2)
        else:
            return 0
    '''

'''
Created on 2019年10月30日

@author: picc
'''
import math
from com.xiaoda.stock.strategies.tradeStrategy.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import *

class SimplePlusSMAStrategy(StrategyParent):
    '''
    将基础的策略与SMA策略相结合
    在跌幅达到一定程度，且股价开始上穿MA20的情况下才进行买入
    '''
    
    
    #决定买入或卖出的数量
    #正数代表买入，负数代表卖出
    #continuousRiseOrFallCnt，正数代表连续上涨，负数代表连续下跌
    def getShareToBuyOrSell(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseOrFallCnt,
                     stock_k_data,todayDate):
            return 0
    

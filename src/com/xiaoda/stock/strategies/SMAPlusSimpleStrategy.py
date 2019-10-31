'''
Created on 2019年10月31日

@author: picc
'''
import math
from com.xiaoda.stock.strategies.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import *

class SimplePlusSMAStrategy(StrategyParent):
    '''
    将SMA策略与基础的策略相结合
    在涨幅达到一定程度时，不论MA20时什么状况，都要进行卖出
    '''
    
    
    #决定买入或卖出的数量
    #正数代表买入，负数代表卖出
    #continuousRiseOrFallCnt，正数代表连续上涨，负数代表连续下跌
    def getShareToBuyOrSell(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseOrFallCnt,
                     stock_hist_data,todayDate):

        return 0

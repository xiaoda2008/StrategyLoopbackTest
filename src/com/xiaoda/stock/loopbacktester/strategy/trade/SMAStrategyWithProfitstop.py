'''
Created on 2019年10月29日

@author: picc
'''
import pandas
import math
from com.xiaoda.stock.loopbacktester.strategy.trade.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import nShare


class SMAStrategyWithProfitstop(StrategyParent):
    '''
    SMA+止盈策略
            根据MA的数值变化情况，决定是否买入或者卖出
            同时考虑持仓收益情况，当持仓收益超过20%时，全部卖出
            进入下一策略周期
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.name="SMAStrategyPlusProfitstop"
 
#可参考的文章：https://www.jianshu.com/p/642ad8a0366e


    #决定买入或卖出的数量
    #正数代表买入，负数代表卖出
    #continuousRiseOrFallCnt，正数代表连续上涨，负数代表连续下跌
    #MA策略不考虑是否第一天，都按照同样的逻辑进行判断
    def getShareToBuyOrSell(self,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseOrFallCnt,
                     stock_k_data,todayDate):
        return 0
        

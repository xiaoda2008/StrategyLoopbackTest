'''
Created on 2019年10月28日

@author: picc
'''
import math
from com.xiaoda.stock.strategies.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import *



class Strategy2(StrategyParent):
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
    

    #决定应买入的数量
    def getShareToBuy(self, priceNow, latestDealPrice, 
                     latestDealType, holdShares,
                     holdAvgPrice, continuousFallCnt):
        
        if priceNow > (1-DOWNRATE)*latestDealPrice:
            return 0
        elif continuousFallCnt==0:
            return math.floor(nShare/6)
        elif continuousFallCnt==1:
            return math.floor(nShare/3)
        else:
            return math.floor(nShare/2)
    
    #决定应当卖出的数量
    def getShareToSell(self, priceNow, latestDealPrice, 
                      latestDealType, holdShares,
                      holdAvgPrice, continuousRiseCnt):
        
        if priceNow < (1+UPRATE)*holdAvgPrice or \
            priceNow < latestDealPrice*(1+UPRATE) or holdShares == 0 :
            return 0
        elif continuousRiseCnt==0:
            return math.floor(holdShares/3)
        elif continuousRiseCnt==1:
            return math.floor(holdShares/2)
        else:
            return holdShares

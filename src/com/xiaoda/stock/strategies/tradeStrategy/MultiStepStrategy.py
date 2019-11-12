'''
Created on 2019年10月28日

@author: picc
'''
import math
from com.xiaoda.stock.strategies.tradeStrategy.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import *



class MultiStepStrategy(StrategyParent):
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
    
    
    #决定买入或卖出的数量
    #正数代表买入，负数代表卖出
    #continuousRiseOrFallCnt，正数代表连续上涨，负数代表连续下跌
    def getShareToBuyOrSell(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseOrFallCnt,
                     stock_k_data,todayDate):
        
        #MultiStep策略第一天，直接进行买入
        if latestDealPrice==0 and latestDealType==0:
            return nShare
        
        
        if priceNow < (1-DOWNRATE)*latestDealPrice:
            #如果下跌超线，应当买入
            if continuousRiseOrFallCnt>=0:
                #此前为上涨或未超线
                return math.floor(nShare/6)
            elif continuousRiseOrFallCnt==-1:
                #此前已连续1次下跌超线
                return math.floor(nShare/3)
            else:
                #此前已2次及以上下跌超线
                return math.floor(nShare/2)
        
        elif priceNow > (1+UPRATE)*holdAvgPrice and priceNow > latestDealPrice*(1+UPRATE) and holdShares>0:
            #如果上涨超线，应当卖出
            if continuousRiseOrFallCnt<=0:
                #此前为下跌或未超线
                return -1*math.floor(holdShares/3)
            elif continuousRiseOrFallCnt==1:
                #此前已连续下跌1次
                return -1*math.floor(holdShares/2)
            else:
                return -1*holdShares
        
        else:
            #未上涨或下跌超线
            return 0
    

'''
Created on 2019年10月28日

@author: picc
'''
import math
from com.xiaoda.stock.loopbacktester.strategy.trade.StrategyParent import StrategyParent
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
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="MultiStepStrategy"
    
    #决定买入或卖出的数量
    #正数代表买入，负数代表卖出
    #continuousRiseOrFallCnt，正数代表连续上涨，负数代表连续下跌
    def getShareAndPriceToBuyOrSell(self,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseOrFallCnt,
                     stock_k_data,todayDate):
        
        stock_k_data = stock_k_data.set_index('trade_date')
               
        openPrice=float(stock_k_data.at[todayDate,'open'])
        closePrice=float(stock_k_data.at[todayDate,'close'])
        highPrice=float(stock_k_data.at[todayDate,'high'])
        lowPrice=float(stock_k_data.at[todayDate,'low'])
        avgPrice=(highPrice+lowPrice)/2
        
         #MultiStep策略第一天，直接进行买入
        if latestDealPrice==0 and latestDealType==0:
            return nShare, avgPrice
               
        #todayMA20 = float(stock_k_data.at[todayDate,'today_MA20'])
        pre_MA20=float(stock_k_data.at[todayDate,'pre_MA20'])
        pre_high=float(stock_k_data.at[todayDate,'pre_high'])
        pre_low=float(stock_k_data.at[todayDate,'pre_low'])
        pre_close=float(stock_k_data.at[todayDate,'pre_close'])
        

        
        if lowPrice < (1-DOWNRATE)*latestDealPrice:
            #如果下跌超线，应当买入
            if continuousRiseOrFallCnt>=0:
                #此前为上涨或未超线
                return math.floor(nShare/6), (1-DOWNRATE)*latestDealPrice
            elif continuousRiseOrFallCnt==-1:
                #此前已连续1次下跌超线
                return math.floor(nShare/3), (1-DOWNRATE)*latestDealPrice
            else:
                #此前已2次及以上下跌超线
                return math.floor(nShare/2), (1-DOWNRATE)*latestDealPrice
        
        elif highPrice > (1+UPRATE)*holdAvgPrice and highPrice > latestDealPrice*(1+UPRATE) and holdShares>0:
            #如果上涨超线，应当卖出
            if continuousRiseOrFallCnt<=0:
                #此前为下跌或未超线
                return -1*math.ceil(holdShares/3), min((1+UPRATE)*holdAvgPrice,latestDealPrice*(1+UPRATE))
            elif continuousRiseOrFallCnt==1:
                #此前已连续下跌1次
                return -1*math.ceil(holdShares/2), min((1+UPRATE)*holdAvgPrice,latestDealPrice*(1+UPRATE))
            else:
                return -1*holdShares, min((1+UPRATE)*holdAvgPrice,latestDealPrice*(1+UPRATE))
        
        else:
            #未上涨或下跌超线
            return 0, 0
    

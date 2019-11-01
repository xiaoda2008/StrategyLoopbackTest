'''
Created on 2019年10月29日

@author: picc
'''
import math
from com.xiaoda.stock.strategies.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import *

class SMAStrategy(StrategyParent):
    '''
    SMA（简单移动平均）策略
            根据MA的数值变化情况，决定是否买入或者卖出
    '''

 
#可参考的文章：https://www.jianshu.com/p/642ad8a0366e


    #决定买入或卖出的数量
    #正数代表买入，负数代表卖出
    #continuousRiseOrFallCnt，正数代表连续上涨，负数代表连续下跌
    def getShareToBuyOrSell(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseOrFallCnt,
                     stock_k_data,todayDate):
        
        stock_data = stock_k_data.set_index('trade_date')
        todayMA20 = stock_data.at[todayDate,'MA20']
        
        stock_data['close_shift']=stock_data['close'].shift(1)

        #需要调整，当天，只可能知道当天开盘价，无法知道当天平均价，不能采用上帝模式

        if stock_data.at[todayDate,'close_shift']<todayMA20 and priceNow>todayMA20:
            #前一天收盘价格低于20日均线，且当天价格高于20时均线-》上穿20日均线，可以买入
            return math.floor(nShare/2)
        elif stock_data.at[todayDate,'close_shift']>todayMA20 and priceNow<todayMA20:
            #前一天收盘价格高于20日均线，且当天价格低于20时均线-》下穿20日均线，可以卖出
            return -1*math.floor(nShare/2)
        else:
            return 0
        
        
    '''
    #决定应买入的数量
    def getShareToBuy(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousFallCnt,
                     stock_hist_data,todayDate):

        todayMA20 = stock_hist_data.at[todayDate,'ma20']
        
        stock_hist_data['close_shift']=stock_hist_data['close'].shift(1)


#需要调整，当天，只可能知道当天开盘价，无法知道当天平均价，不能采用上帝模式


        if stock_hist_data.at[todayDate,'close_shift']<todayMA20 and priceNow>todayMA20:
            #前一天收盘价格低于20日均线，且当天价格高于20时均线-》上穿20日均线，可以买入
            return math.floor(nShare/2)
        else:
            return 0
        
    
    #决定应当卖出的数量
    def getShareToSell(self,priceNow,latestDealPrice, 
                      latestDealType,holdShares,
                      holdAvgPrice,continuousRiseCnt,
                      stock_hist_data,todayDate):
        
        todayMA20 = stock_hist_data.at[todayDate,'ma20']   
        stock_hist_data['close_shift']=stock_hist_data['close'].shift(1)
        
        if stock_hist_data.at[todayDate,'close_shift']>todayMA20 and priceNow<todayMA20:
            #前一天收盘价格高于20日均线，且当天价格低于20时均线-》下穿20日均线，可以卖出
            return math.floor(nShare/2)
        else:
            return 0
    '''

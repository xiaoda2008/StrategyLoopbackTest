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

 

    #决定应买入的数量
    def getShareToBuy(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousFallCnt,
                     stock_hist_data,todayDate):
        
        todayMA5 = stock_hist_data.at[todayDate,'ma5']
        todayMA10 = stock_hist_data.at[todayDate,'ma10']
        todayMA20 = stock_hist_data.at[todayDate,'ma20']   
         

        return 0
        
    
    #决定应当卖出的数量
    def getShareToSell(self,priceNow,latestDealPrice, 
                      latestDealType,holdShares,
                      holdAvgPrice,continuousRiseCnt,
                      stock_hist_data,todayDate):
        
        todayMA5 = stock_hist_data.at[todayDate,'ma5']
        todayMA10 = stock_hist_data.at[todayDate,'ma10']
        todayMA20 = stock_hist_data.at[todayDate,'ma20']   
        
        return 0
    
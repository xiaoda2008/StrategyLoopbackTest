'''
Created on 2019年10月28日

@author: picc
'''

class StrategyParent:
    
    def __init__(self,name):
        self.name=name
    
    def getStrategyName(self):
        return self.name
    
    
    #可以考虑将买入/卖出进行合并，返回值直接使用正负号代表买入/卖出
    
    
    #决定买入或卖出的数量
    #正数代表买入，负数代表卖出
    #continuousRiseOrFallCnt，正数代表连续上涨，负数代表连续下跌
    def getShareToBuyOrSell(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseOrFallCnt,
                     stock_hist_data,todayDate):
        return 0
'''    
    #决定应买入的数量
    def getShareToBuy(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseCnt,
                     stock_hist_data,todayDate):
        
        return 0
    
    #决定应当卖出的数量
    def getShareToSell(self,priceNow,latestDealPrice, 
                      latestDealType,holdShares,
                      holdAvgPrice,continuousFallCnt,
                      stock_hist_data,todayDate):
        return 0
'''    
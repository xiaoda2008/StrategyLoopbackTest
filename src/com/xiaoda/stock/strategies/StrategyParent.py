'''
Created on 2019年10月28日

@author: picc
'''

class StrategyParent:
    
    def __init__(self,name):
        self.name=name
    
    def getStrategyName(self):
        return self.name
    
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
    
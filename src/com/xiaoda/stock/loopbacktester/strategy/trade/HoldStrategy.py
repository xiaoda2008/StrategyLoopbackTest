'''
Created on 2019年12月16日

@author: xiaoda
'''
from com.xiaoda.stock.loopbacktester.strategy.trade.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
import os
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import nShare

class HoldStrategy(StrategyParent):
    '''
    classdocs
    '''

    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self):
        '''
        Constructor
        '''   
        self.name="HoldStrategy"
    
    def getShareAndPriceToBuyOrSell(self,latestDealPrice, 
                 latestDealType,holdShares,
                 holdAvgPrice,continuousRiseOrFallCnt,
                 stockCode,stockInd,stock_k_data,todayDate):
        
        #openPrice=float(stock_k_data.at[todayDate,'open'])
        #closePrice=float(stock_k_data.at[todayDate,'close'])
        highPrice=float(stock_k_data.at[todayDate,'high'])
        lowPrice=float(stock_k_data.at[todayDate,'low'])
        avgPrice=(highPrice+lowPrice)/2
 
 
        #Simple策略第一天，直接以当日平均价格进行买入
        if latestDealPrice==0 and latestDealType==0:
            return nShare,avgPrice
        else:
            return 0,0
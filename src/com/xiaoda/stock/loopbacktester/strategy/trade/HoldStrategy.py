'''
Created on 2019年12月16日

@author: xiaoda
'''
from com.xiaoda.stock.loopbacktester.strategy.trade.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger

class HoldStrategy(StrategyParent):
    '''
    classdocs
    '''

    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self, params):
        '''
        Constructor
        '''   
        self.name="HoldStrategy"
    
    def getShareAndPriceToBuyOrSell(self,latestDealPrice, 
                 latestDealType,holdShares,
                 holdAvgPrice,continuousRiseOrFallCnt,
                 stock_k_data,todayDate):
        return 0,0
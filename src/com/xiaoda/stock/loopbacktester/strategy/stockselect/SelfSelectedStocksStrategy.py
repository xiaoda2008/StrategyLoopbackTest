'''
Created on 2019年11月12日

@author: picc
'''
import os
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from numpy.distutils.log import good


class SelfSelectedStocksStrategy(StrategyParent):
    '''
    classdocs
    '''
    
    
    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="SelfSelectedStocksStrategy"
        self.finProcessor=FinanceDataProcessor()
    
    #决定对哪些股票进行投资
    def getSelectedStockList(self,sdProcessor,startdateStr):
   
        sdict=sdProcessor.getSelfSelectedStockList()
        #sdict=sdProcessor.getSH50Dict()
        #sdict=sdProcessor.getSZ100Dict()
        #sdict=sdProcessor.getHS300Dict()

        returnStockList=list(sdict.keys())
        
        return returnStockList

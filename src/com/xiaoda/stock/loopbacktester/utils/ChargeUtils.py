'''
Created on 2019年10月28日

@author: picc
'''
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import SecChargeRate



class ChargeUtils:

    
    @staticmethod
    def getBuyCharge(totalDealAmt):
        #买入需要交手续费：
        #1、过户费：交易金额*0.00002
        #2、交易所规费：交易金额*0.0000687
        #3、券商佣金：如果（交易金额*佣金比例+交易所规费）<5元，交易所规费与佣金共取5元，如大于5元，为交易金额*佣金比例+交易所规费
        totalCharge = 0
        totalCharge += totalDealAmt*(0.00002)
        
        if totalDealAmt*(SecChargeRate+0.0000687) < 5:
            totalCharge += 5
        else:
            totalCharge += totalDealAmt*(SecChargeRate++0.0000687)
            
        return totalCharge
        
    
    @staticmethod
    def getSellCharge(totalDealAmt):
        #卖出需要交手续费：
        #1、过户费：交易金额*0.00002
        #2、交易所规费：交易金额*0.0000687
        #3、印花税：交易金额*0.001
        #3、券商佣金：如果（交易金额*佣金比例+交易所规费）<5元，交易所规费与佣金共取5元，如大于5元，为交易金额*佣金比例+交易所规费
        totalCharge = 0
        totalCharge += totalDealAmt*(0.00002+0.001)
        
        if totalDealAmt*(SecChargeRate+0.0000687) < 5:
            totalCharge += 5
        else:
            totalCharge += totalDealAmt*(SecChargeRate+0.0000687)
            
        return totalCharge


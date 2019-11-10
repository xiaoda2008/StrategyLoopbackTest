'''
Created on 2019年11月9日

@author: picc
'''
from scipy import optimize

class IRRProcessor:
    '''
    classdocs
    '''

    # 函数
    @staticmethod
    def xnpv(rate, cashflows):
        return sum([cf/(1+rate)**((t-cashflows[0][0]).days/365.0) for (t,cf) in cashflows])
     
    @staticmethod
    def xirr(cashflows, guess=0.1):
        try:
            return optimize.newton(lambda r: IRRProcessor.xnpv(r,cashflows),guess)
        except:
            print('Calc Wrong')
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

    # 函数
    @staticmethod
    def xirr2(cashflows):
        years = [(ta[0] - cashflows[0][0]).days / 365. for ta in cashflows]
        residual = 1.0
        step = 0.05
        guess = 0.1
        epsilon = 0.0001
        limit = 50000
        while abs(residual) > epsilon and limit > 0:
            limit -= 1
            residual = 0.0
            for i, trans in enumerate(cashflows):
                residual += trans[1] / pow(guess, years[i])
            if abs(residual) > epsilon:
                if residual > 0:
                    guess += step
                else:
                    guess -= step
                    step /= 2.0
        return guess - 1

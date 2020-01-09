'''
Created on 2020年1月8日

@author: xiaoda
'''

class RiskAvoidProcessor(object):
    '''
    防暴雷逻辑
    '''
    
    def __init__(self, params):
        '''
        Constructor
        '''   
    @staticmethod
    def getRiskAvoidFlg(stockCode,ic,bs,cf,sdProcessor):

        totalAsset=bs[bs['total_assets'].notnull()].reset_index(drop=True).at[0,'total_assets']
        
        try:
            #商誉
            goodwill=bs[bs['goodwill'].notnull()].reset_index(drop=True).at[0,'goodwill']      
        except:
            goodwill=0
                    
        if goodwill/totalAsset>0.5:
            return True        


        sData=sdProcessor.getStockInfo(stockCode)
        
        ind=sData.at[0,'industry']
        if ind in ['证券','银行']:
            return False

        #货币资金
        moneyCap=bs[bs['money_cap'].notnull()].reset_index(drop=True).at[0,'money_cap']
        
        
        #短期借款
        if bs[bs['st_borr'].notnull()].empty:
            stBorr=0
        else:
            stBorr=bs[bs['st_borr'].notnull()].reset_index(drop=True).at[0,'st_borr']
        
        #长期借款
        if bs[bs['lt_borr'].notnull()].empty:
            ltBorr=0
        else:
            ltBorr=bs[bs['lt_borr'].notnull()].reset_index(drop=True).at[0,'lt_borr']
        
        #应付债券
        if bs[bs['bond_payable'].notnull()].empty:
            bondPayable=0
        else:
            bondPayable=bs[bs['bond_payable'].notnull()].reset_index(drop=True).at[0,'bond_payable']
        
        #总资产
        totalAsset=bs[bs['total_assets'].notnull()].reset_index(drop=True).at[0,'total_assets']
        
        cashRatio=moneyCap/totalAsset
        
        liabRatio=(stBorr+ltBorr+bondPayable)/moneyCap
        
        if liabRatio>0.5 and cashRatio>0.25:
            return True     
              
        return False
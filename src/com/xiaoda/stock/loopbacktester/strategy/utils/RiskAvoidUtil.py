'''
Created on 2020年1月8日

@author: xiaoda
'''

class RiskAvoidProcessor(object):
    '''
    防暴雷逻辑
    根据股票的财务等情况，确定是否应当剔除
    如果是True，说明应当被剔除
    
    '''
    
    def __init__(self, params):
        '''
        Constructor
        '''   
    @staticmethod
    def getRiskAvoidFlg(stockCode,ic,bs,cf,sdProcessor):

        totalAsset=float(bs[bs['total_assets'].notnull()].reset_index(drop=True).at[0,'total_assets'])
        
        try:
            #商誉
            goodwill=float(bs[bs['goodwill'].notnull()].reset_index(drop=True).at[0,'goodwill'])   
        except:
            goodwill=0
                    
        if goodwill/totalAsset>0.6:
            return True        


        sData=sdProcessor.getStockInfo(stockCode)
        
        ind=sData.at[0,'industry']
        if ind in ['证券','银行']:
            return False

        #货币资金
        moneyCap=float(bs[bs['money_cap'].notnull()].reset_index(drop=True).at[0,'money_cap'])
        
        
        #短期借款
        if bs[bs['st_borr'].notnull()].empty:
            stBorr=0
        else:
            stBorr=float(bs[bs['st_borr'].notnull()].reset_index(drop=True).at[0,'st_borr'])
        
        #长期借款
        if bs[bs['lt_borr'].notnull()].empty:
            ltBorr=0
        else:
            ltBorr=float(bs[bs['lt_borr'].notnull()].reset_index(drop=True).at[0,'lt_borr'])
        
        #应付债券
        if bs[bs['bond_payable'].notnull()].empty:
            bondPayable=0
        else:
            bondPayable=float(bs[bs['bond_payable'].notnull()].reset_index(drop=True).at[0,'bond_payable'])
        
        #总资产
        totalAsset=bs[bs['total_assets'].notnull()].reset_index(drop=True).at[0,'total_assets']
        
        cashRatio=moneyCap/totalAsset
        
        liabRatio=(stBorr+ltBorr+bondPayable)/moneyCap
        
        if liabRatio>0.5 and cashRatio>0.25:
            return True     
              
        return False
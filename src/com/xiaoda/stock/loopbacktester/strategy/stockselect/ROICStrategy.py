'''
Created on 2020年7月15日

@author: xiaoda
'''
import os
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.strategy.utils.RiskAvoidUtil import RiskAvoidProcessor
from com.xiaoda.stock.loopbacktester.strategy.utils.StockListFilterUtil import StockListFilterProcessor

class ROICStrategy(StrategyParent):
    '''
    根据股票ROIC进行选股，选择ROIC排在前5%的股票进行交易
    '''
    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="ROICStrategy"
        self.finProcessor=FinanceDataProcessor()

    #决定对哪些股票进行投资
    def getSelectedStockList(self,sdProcessor,startdateStr):
        
        sdict=sdProcessor.getHS300Dict()
        ROICDict={}
        
        for (stockCode,scdict) in sdict.items():
            if stockCode=="600519.SH":
                continue
                        
            listdate=scdict['list_date']
            
            if listdate>startdateStr:
                continue



            #ROIC=税后营运收入/总资本投入
            #从资产负债表获取净资产数据
            
            bs=self.finProcessor.getLatestBalanceSheetReport(stockCode,startdateStr,False)
            #bs为所有之前发布的所有资产负债表数据
            
            ic=self.finProcessor.getLatestIncomeReport(stockCode,startdateStr,False)
            #ic为之前发布的所有利润表数据
            
            #获取现金流量表中，现金等价物总数
            cf=self.finProcessor.getLatestCashFlowReport(stockCode,startdateStr,False)
            #cf为之前发布的所有现金流量表数据
            
            #有可能数据不全，直接跳过
            if bs.empty or ic.empty:
                continue
            

            
            
            #print(stockCode)
            
            #if(stockCode=='000423.SZ'):
            #    print()

            #金融类企业，没有财务费用
            if ic[ic['fin_exp'].notnull()].empty:
                continue

            
            #营业利润
            operate_profit=ic[ic['operate_profit'].notnull()].reset_index(drop=True).at[0,'operate_profit']
            #财务费用
            fin_exp=ic[ic['fin_exp'].notnull()].reset_index(drop=True).at[0,'fin_exp']
            #投资收益
            invest_income=ic[ic['invest_income'].notnull()].reset_index(drop=True).at[0,'invest_income']
            #所得税费用
            income_tax=ic[ic['income_tax'].notnull()].reset_index(drop=True).at[0,'income_tax']
            #税后营运收入
            NOPLAT=operate_profit-fin_exp-invest_income-income_tax

            
            #期初权益
            total_hldr_eqy_inc_min_int=bs[bs['total_hldr_eqy_inc_min_int'].notnull()].reset_index(drop=True).at[1,'total_hldr_eqy_inc_min_int']

            #期初短期借款
            if len(bs[bs['st_borr'].notnull()])<=1:
                st_borr=0
            else:
                st_borr=bs[bs['st_borr'].notnull()].reset_index(drop=True).at[1,'st_borr']
            
            #期初一年内到期的长期负债
            if len(bs[bs['non_cur_liab_due_1y'].notnull()])<=1:
                non_cur_liab_due_1y=0
            else:
                non_cur_liab_due_1y=bs[bs['non_cur_liab_due_1y'].notnull()].reset_index(drop=True).at[1,'non_cur_liab_due_1y']  
            
            #期初长期借款
            if len(bs[bs['lt_borr'].notnull()])<=1:
                lt_borr=0
            else:
                lt_borr=bs[bs['lt_borr'].notnull()].reset_index(drop=True).at[1,'lt_borr']  
            
            #期初应付债券
            if len(bs[bs['bond_payable'].notnull()])<=1:
                bond_payable=0
            else:
                bond_payable=bs[bs['bond_payable'].notnull()].reset_index(drop=True).at[1,'bond_payable']
            
            #期初长期应付款
            if len(bs[bs['lt_payable'].notnull()])<=1:
                lt_payable=0
            else:            
                lt_payable=bs[bs['lt_payable'].notnull()].reset_index(drop=True).at[1,'lt_payable']
            
            #有息负债＝短期借款＋一年内到期的长期负债＋长期借款＋应付债券＋长期应付款
            liabWithInt=st_borr+non_cur_liab_due_1y+lt_borr+bond_payable+lt_payable
            
            

            #货币资金
            money_cap=bs[bs['money_cap'].notnull()].reset_index(drop=True).at[0,'money_cap']
            #流动负债合计
            total_cur_liab=bs[bs['total_cur_liab'].notnull()].reset_index(drop=True).at[0,'total_cur_liab']
            #流动资产合计
            total_cur_assets=bs[bs['total_cur_assets'].notnull()].reset_index(drop=True).at[0,'total_cur_assets']            
            #交易性金融资产
            if len(bs[bs['trad_asset'].notnull()])<=1:
                trad_asset=0
            else:                      
                trad_asset=bs[bs['trad_asset'].notnull()].reset_index(drop=True).at[0,'trad_asset']

            #超额现金
            extCash=max(0,float(money_cap)+float(trad_asset))-max(0,float(total_cur_liab)-(float(total_cur_assets)-(float(money_cap)+float(trad_asset))))


            #总资本投入
            InvestCap=total_hldr_eqy_inc_min_int-liabWithInt-extCash
            


            #资本回报率（ROIC）= （净收入-税收） / 总资本 = 税后营运收入/ （总财产 - 过剩现金- 无息流动负债）。
            #超额现金 = Max(0, (货币资金 + 交易性金融资产) - Max(0, 流动负债合计 - (流动资产合计 - (货币资金 + 交易性金融资产))))





            #防暴雷、防财务造假逻辑
            if RiskAvoidProcessor.getRiskAvoidFlg(stockCode, ic, bs, cf, sdProcessor)==True:
                continue
             
        
            if bs.empty or ic.empty:
                continue
            else: 
                #ROIC
                ROIC=NOPLAT/InvestCap
    
            ROICDict[stockCode]=ROIC
            
            self.log.logger.info('ROICStrategy:'+stockCode+','+str(ROIC))

        sortedROICList=sorted(ROICDict.items(),key=lambda x:x[1],reverse=True)

        tmpStockList=[]

        for tscode, ROIC in sortedROICList[:30]:
            tmpStockList.append(tscode)


        #删选以避免某一行业占比过高
        returnStockList=StockListFilterProcessor.filterStockList(tmpStockList, sdProcessor)        

        #选出的股票不要少于10支
        if len(returnStockList)<10:
            tmpStockList=[]
            for tscode, ratio in sortedROICList[:50]:
                tmpStockList.append(tscode)
            
            #删选以避免某一行业占比过高
            returnStockList=StockListFilterProcessor.filterStockList(tmpStockList, sdProcessor)      
 
             
        return returnStockList






'''

#使用TuShare pro版本    
tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
pro = tushare.pro_api()

#df = pro.query('fina_indicator', ts_code='000001.SZ', start_date='20170101', end_date='20180801',fields='roic')
#print(df)


trad_asset=pro.balancesheet(ts_code='600000.SH', start_date='20180101', end_date='20180430', fields='trad_asset')


ta=trad_asset.at[0,'trad_asset']

trad_asset=max(0,ta)

'''

'''
资本bai回报率（ROIC）= （净收入-税收） / 总资本 = 税后du营运收入/ （总财产 - 过剩现zhi金- 无息流动负债）。

超额现金 = Max(0, (货币资金 + 交易性金融资产) - Max(0, 流动负债合计 - (流动资产合计 - (货币资金 + 交易性金融资产))))
'''

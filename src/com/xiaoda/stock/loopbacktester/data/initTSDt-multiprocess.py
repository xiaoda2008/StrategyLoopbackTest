'''
Created on 2019年11月2日

@author: xiaoda

一次性的获取所有需要的数据到本地数据库中
并在过程中进行处理情况的记录，以便kettle进行错误处理
'''

import sys
import os

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
import traceback
#print(sys.path)
#sys.path.clear()
#print(sys.path)
sys.path.append(r'E:\eclipse-workspace\StrategyLoopbackTest\src')
sys.path.append(r'D:\Program Files\Python38\Lib\site-packages')
sys.path.append(r'D:\Program Files\Python38')
sys.path.append(r'D:\Program Files\Python38\DLLs')
sys.path.append(r'D:\Program Files\Python38\libs')
sys.path.append(r'C:\Users\picc\eclipse-workspace\StrategyLoopbackTester\src')
sys.path.append(r'C:\Users\picc\AppData\Local\Programs\Python\Python36-32\Lib\site-packages')
sys.path.append(r'C:\Users\picc\AppData\Local\Programs\Python\Python36-32')



from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
import tushare
from sqlalchemy.util.langhelpers import NoneType
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
#from com.xiaoda.stock.loopbacktester.utils.ParamUtils import LOGGINGDIR
from datetime import datetime as dt
import multiprocessing
from multiprocessing import Manager
import sqlalchemy
import argparse
import time

DAYONE='19900101'
log=Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')


def get8slListFromStockList(stockList):
    retSLList=[]
    slLen=len(stockList)
    #对所有股票进行遍历
    i=0
    tmpList1=[]
    tmpList2=[]
    tmpList3=[]
    tmpList4=[]
    tmpList5=[]
    tmpList6=[]
    tmpList7=[]
    tmpList8=[]
    
    for stockCode in stockList:
        #log.logger.info(stockCode)
        if i<slLen*1/8:
            tmpList1.append(stockCode)
        elif i>=slLen*1/8 and i<slLen*2/8:
            tmpList2.append(stockCode)
        elif i>=slLen*2/8 and i<slLen*3/8:
            tmpList3.append(stockCode)
        elif i>=slLen*3/8 and i<slLen*4/8:
            tmpList4.append(stockCode)
        elif i>=slLen*4/8 and i<slLen*5/8:
            tmpList5.append(stockCode)
        elif i>=slLen*5/8 and i<slLen*6/8:
            tmpList6.append(stockCode)
        elif i>=slLen*6/8 and i<slLen*7/8:
            tmpList7.append(stockCode)
        else:
            tmpList8.append(stockCode)            
        i=i+1
    retSLList.append(tmpList1)
    retSLList.append(tmpList2)
    retSLList.append(tmpList3)
    retSLList.append(tmpList4)
    retSLList.append(tmpList5)
    retSLList.append(tmpList6)
    retSLList.append(tmpList7)
    retSLList.append(tmpList8)
                
    return retSLList 

def getlastquarterfirstday():
        today=dt.now()
        quarter = (today.month-1)/3+1
        if quarter == 1:
            return dt(today.year-1,10,1)
        elif quarter == 2:
            return dt(today.year,1,1)
        elif quarter == 3:
            return dt(today.year,4,1)
        else:
            return dt(today.year,7,1)
    
'''
def partialUpdate(mysqlSession):    
    #部分更新语句
    pupdatesql="update u_data_desc set content='%s' where content_name='last_update_time';"%(dt.now().strftime('%Y%m%d %H:%M:%S'))
    MysqlProcessor.execSql(mysqlSession,pupdatesql,True)
'''
       
def totalUpdate(mysqlSession,sdProcessor):
    #全局更新语句
    tupdatesql="update u_data_desc set content='%s' where content_name='last_total_update_time';"%(dt.now().strftime('%Y%m%d %H:%M:%S'))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    tupdatesql="update u_data_desc set content='%s' where content_name='finance_report_date_update_to';"%(dt.now().strftime('%Y%m%d'))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    #从sd往后找到第一个交易日，含sd
    tupdatesql="update u_data_desc set content='%s' where content_name='data_start_dealday';"%(sdProcessor.getNextDealDay(sd, True))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    #从ed往前找到最后一个交易日，是否含ed需要根据当前时间是否已经完成该日交易
    #最好是每天取前一个交易日的数据，这样就不会存在当天日期是否已经可用的问题
    tupdatesql="update u_data_desc set content='%s' where content_name='data_end_dealday';"%(sdProcessor.getLastDealDay(ed,True))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    mysqlSession.commit()
    
    
'''
#不应当这样保存，而是应当建立一个表，专门用0，1表示数据的完整性
#可以在u_stock_list表增加一列，用0，1表示数据是否已全量获取
def lastDataUpdate(mysqlSession,stockCode,dataType):
    #if dataType=='FR_StockCode':
    #    #更新财务报表最新股票代码
    #    sql="update u_data_desc set content='%s' where content_name='finance_report_stockcode_update_to';"%(stockCode)
    if dataType=='FR_Date':
        sql="update u_data_desc set content='%s' where content_name='finance_report_date_update_to';"%(dt.now().strftime('%Y%m%d'))
    #elif dataType=="KD":
    #    #更新K线最新股票代码
    #    sql="update u_data_desc set content='%s' where content_name='kdata_stockcode_update_to';"%(stockCode)
    #elif dataType=='DB':
    #    #更新股票每日基本数据
    #    sql="update u_data_desc set content='%s' where content_name='dailybasic_stockcode_update_to';"%(stockCode)
    #elif dataType=="ADJ":
    #    #更新复权因子最新股票代码
    #    sql="update u_data_desc set content='%s' where content_name='adjdata_stockcode_update_to';"%(stockCode)
    #else:
    #    raise Exception("dataType不正确")
    
    MysqlProcessor.execSql(mysqlSession,sql,True)
'''

def processStockDataGet(stockList,startday,endday):

    #使用TuShare pro版本    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    sdDataAPI = tushare.pro_api()
    #写入数据库的引擎    
    mysqlProcessor=MysqlProcessor()
    mysqlEngine=mysqlProcessor.getMysqlEngine()
    mysqlSession=mysqlProcessor.getMysqlSession()
    
        
    ix=0
    length=len(stockList)
    while ix<length:
       
        stockCode=stockList[ix]

        
        try:
            #3.1写入财务报表
            #获取资产负债表
            bs=sdDataAPI.balancesheet(ts_code=stockCode,start_date=startday,end_date=endday,fields=
                'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,total_share,cap_rese,\
                undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,\
                oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,\
                loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,\
                total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,\
                oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,\
                oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,\
                decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,\
                deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,\
                refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,\
                transac_seat_fee,invest_as_receiv,total_assets,lt_borr,st_borr,cb_borr,depos_ib_deposits,\
                loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,\
                comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,\
                acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,\
                acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,\
                bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,\
                defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,\
                agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,\
                reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,\
                indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,\
                forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,\
                total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,\
                oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,\
                payables,hfs_assets,hfs_sales,update_flag') 
            bs.to_sql(name='s_finreport_balancesheet_'+stockCode[:6],
                      con=mysqlEngine,chunksize=100,if_exists='replace',index=None)
            #获取现金流量表
            cf = sdDataAPI.cashflow(ts_code=stockCode,start_date=startday,end_date=endday,fields=
                'ts_code,ann_date,f_ann_date,end_date,comp_type,report_type,net_profit,finan_exp,\
                c_fr_sale_sg,recp_tax_rends,n_depos_incr_fi,n_incr_loans_cb,n_inc_borr_oth_fi,\
                prem_fr_orig_contr,n_incr_insured_dep,n_reinsur_prem,n_incr_disp_tfa,ifc_cash_incr,\
                n_incr_disp_faas,n_incr_loans_oth_bank,n_cap_incr_repur,c_fr_oth_operate_a,\
                c_inf_fr_operate_a,c_paid_goods_s,c_paid_to_for_empl,c_paid_for_taxes,\
                n_incr_clt_loan_adv,n_incr_dep_cbob,c_pay_claims_orig_inco,pay_handling_chrg,\
                pay_comm_insur_plcy,oth_cash_pay_oper_act,st_cash_out_act,n_cashflow_act,\
                oth_recp_ral_inv_act,c_disp_withdrwl_invest,c_recp_return_invest,\
                n_recp_disp_fiolta, n_recp_disp_sobu,stot_inflows_inv_act,c_pay_acq_const_fiolta,\
                c_paid_invest,n_disp_subs_oth_biz,oth_pay_ral_inv_act,n_incr_pledge_loan,\
                stot_out_inv_act,n_cashflow_inv_act,c_recp_borrow,proc_issue_bonds,\
                oth_cash_recp_ral_fnc_act,stot_cash_in_fnc_act,free_cashflow,c_prepay_amt_borr,\
                c_pay_dist_dpcp_int_exp,incl_dvd_profit_paid_sc_ms,oth_cashpay_ral_fnc_act,\
                stot_cashout_fnc_act,n_cash_flows_fnc_act,eff_fx_flu_cash,n_incr_cash_cash_equ,\
                c_cash_equ_beg_period,c_cash_equ_end_period,c_recp_cap_contrib,incl_cash_rec_saims,\
                uncon_invest_loss,prov_depr_assets,depr_fa_coga_dpba,amort_intang_assets,\
                lt_amort_deferred_exp,decr_deferred_exp,incr_acc_exp,loss_disp_fiolta,\
                loss_scr_fa,loss_fv_chg,invest_loss,decr_def_inc_tax_assets,incr_def_inc_tax_liab,\
                decr_inventories,decr_oper_payable,incr_oper_payable,others,im_net_cashflow_oper_act,\
                conv_debt_into_cap,conv_copbonds_due_within_1y,fa_fnc_leases,end_bal_cash,\
                beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,im_n_incr_cash_equ,update_flag')
            cf.to_sql(name='s_finreport_cashflow_'+stockCode[:6],
                      con=mysqlEngine,chunksize=100,if_exists='replace',index=None)
            #获取利润表
            ic = sdDataAPI.income(ts_code=stockCode,start_date=startday,end_date=endday,fields=
                'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,\
                total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,\
                n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,\
                n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,\
                ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,\
                sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,\
                reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,\
                reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,\
                nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,\
                oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,\
                ebit,ebitda,insurance_exp,undist_profit,distable_profit,update_flag')
            ic.to_sql(name='s_finreport_income_'+stockCode[:6],
                      con=mysqlEngine,chunksize=100,if_exists='replace',index=None)
            
            #3.2写入复权因子
            #获取该股票的复权因子数据并写入数据库
            adj_data = sdDataAPI.adj_factor(ts_code=stockCode,start_date=startday,end_date=endday)
            adj_data.sort_index(inplace=True,ascending=False) 
            #存入数据库
            adj_data.to_sql(name='s_adjdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None) 
            
            
            #3.3写入kdata、dailybasic数据

            
            #标记是否第一次出现
            #因为如果失败重试，需要在第一次出现时replace
            #否则会导致无法重新插入
            kdataFlg=False
            dbFlg=False
            
            #将1990-01-01到1999-12-31该股票数据导入数据库
            if startday>'19991231' or endday<'19900101':
                pass
            else:
                if startday<'19900101':
                    tmpSTARTDATE='19900101'
                else:
                    tmpSTARTDATE=startday
                if endday>'19991231':
                    tmpENDDATE='19991231'
                else:
                    tmpENDDATE=endday
                
                #获取该股票数据并写入数据库
                stock_k_data=tushare.pro_bar(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if stock_k_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE))   
                    #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
                    #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
                else:
                    #该股票出现交易数据，必然是第一次出现，直接替代即可
                    kdataFlg=True
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)

                #获取该股票数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                else:
                    dbFlg=True
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
                    
                
                
            #将2000-01-01到2009-12-31该股票数据导入数据库
            if startday>'20091231' or endday<'20000101':
                pass
            else:
                if startday<'20000101':
                    tmpSTARTDATE='20000101'
                else:
                    tmpSTARTDATE=startday
                if endday>'20091231':
                    tmpENDDATE='20091231'
                else:
                    tmpENDDATE=endday
                
                #获取该股票数据并写入数据库
                stock_k_data=tushare.pro_bar(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if stock_k_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                    #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
                    #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
                elif kdataFlg==False:
                    kdataFlg=True
                    #该股票未出现过交易数据
                    #需要replace
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)                                  
                else:
                    #该股票出现交易数据
                    #且在上一区间已经有过交易，直接增加数据即可
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)


                #获取该股票数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                elif dbFlg==False:
                    dbFlg=True
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
                    
                else:
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)

            
            #将2010-01-01到2018-12-31该股票数据导入数据库
            if startday>'20181231' or endday<'20100101':
                pass
            else:
                if startday<'20100101':
                    tmpSTARTDATE='20100101'
                else:
                    tmpSTARTDATE=startday
                if endday>'20181231':
                    tmpENDDATE='20181231'
                else:
                    tmpENDDATE=endday
            
                #获取该股票数据并写入数据库
                stock_k_data=tushare.pro_bar(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)   
                if stock_k_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                    #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
                    #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
                elif kdataFlg==False:
                    kdataFlg=True
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)                    
                else:
                    #该股票出现交易数据
                    #且在上一区间已经有过交易，直接增加数据即可
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)

                #获取该股票数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                elif dbFlg==False:
                    dbFlg=True
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
                else:
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)

            
            #将2019-01-01以后该股票数据导入数据库
            if startday>dt.now().strftime('%Y%m%d') or endday<'20190101':
                pass
            else:
                if startday<'20190101':
                    tmpSTARTDATE='20190101'
                else:
                    tmpSTARTDATE=startday
                if endday>dt.now().strftime('%Y%m%d'):
                    tmpENDDATE=dt.now().strftime('%Y%m%d')
                else:
                    tmpENDDATE=endday
            
                #获取该股票数据并写入数据库
                stock_k_data=tushare.pro_bar(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)  
                if stock_k_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE)) 
                    #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
                    #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
                elif kdataFlg==False:
                    kdataFlg=True
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)                      
                else:
                    #该股票出现交易数据
                    #且在上一区间已经有过交易，直接增加数据即可
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)               

                #获取该股票数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                elif dbFlg==False:
                    dbFlg=True
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
                else:
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
            
        except Exception as e:#出现异常
            #出现异常，则还需要继续循环，继续对该股票继续处理
            traceback.print_exc()
            log.logger.warning('处理%s的数据时出现异常，重新进行处理'%(stockCode))
            time.sleep(0.3)
            continue
        else:#未出现异常
            log.logger.info('处理完%s的财务报表、kdata、adjdata数据'%(stockCode))
            #partialUpdate(mysqlSession)
            
            #lastDataUpdate(mysqlSession,stockCode, "FR_StockCode")
            #lastDataUpdate(mysqlSession,stockCode, "FR_Date")    
            #lastDataUpdate(mysqlSession,stockCode,"KD")
            #lastDataUpdate(mysqlSession,stockCode, "ADJ")
            #lastDataUpdate(mysqlSession,stockCode, "DB")
                     
            #未出现异常，进行下一个股票的处理
            ix=ix+1
     
if __name__ == '__main__':
 
    #写入数据库的引擎    
    mysqlProcessor=MysqlProcessor()
    mysqlEngine=mysqlProcessor.getMysqlEngine()
    mysqlSession=mysqlProcessor.getMysqlSession()
        
    #使用TuShare pro版本    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    sdDataAPI = tushare.pro_api()

  
    #1、获取交易日信息，并存入数据库
    startday=DAYONE
    
    trade_cal_data=sdDataAPI.trade_cal(exchange='',start_date=startday,end_date=dt.now().strftime('%Y%m%d'),fields='exchange,cal_date,is_open,pretrade_date')
    
    #将交易日列表存入数据库表中
    trade_cal_data.to_sql(name='u_trade_cal',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    
    #完成部分信息更新
    #partialUpdate(mysqlSession)
    
    
    #2、获取股票列表并存入数据库
    sdf=sdDataAPI.stock_basic(exchange='',list_status='L',fields='ts_code,symbol,name,area,industry,list_date')
    
    try:
        #将股票列表存入数据库表中
        sdf.to_sql(name='u_stock_list',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    except sqlalchemy.exc.IntegrityError:
        log.logger.warning("股票列表获取出现错误，可能已经获取过")
        
    stockCodeList=list(sdf['ts_code'])
    #完成部分信息更新
    #partialUpdate(mysqlSession)
    
    
    
    sqlStr='alter table u_stock_list modify column ts_code varchar(20) primary key;'
    try:
        mysqlProcessor.execSql(mysqlSession,sqlStr,True)       
    except sqlalchemy.exc.OperationalError:
        log.logger.warning("修正股票清单表出错，可能已经修正过")



    sqlStr1="create table u_data_desc (content_name varchar(100),content varchar(200) not null default '',comments varchar(300) not null default '');"
    sqlStr2="insert into u_data_desc (content_name,content,comments) values ('last_totally_update_time','','the time of last total data update');"
    sqlStr3="insert into u_data_desc (content_name,content,comments) values ('data_start_dealday','','the start deal day(include) of all data');"
    sqlStr4="insert into u_data_desc (content_name,content,comments) values ('data_end_dealday','','the end deal day(include) of all data');"
    sqlStr5="insert into u_data_desc (content_name,content,comments) values ('finance_report_date_update_to','','the date of last update of finance report');"


    try:
        mysqlProcessor.execSql(mysqlSession,sqlStr1,True)       
        mysqlProcessor.execSql(mysqlSession,sqlStr2,True)       
        mysqlProcessor.execSql(mysqlSession,sqlStr3,True)       
        mysqlProcessor.execSql(mysqlSession,sqlStr4,True)       
        mysqlProcessor.execSql(mysqlSession,sqlStr5,True)       
     
    except sqlalchemy.exc.OperationalError:
        log.logger.warning("初始化u_data_desc表出错")


    
    #sqlStr='alter table u_stock_list add column is_data_available tinyint(1) default 0;'
    #try:
    #    mysqlProcessor.execSql(mysqlSession,sqlStr,True)       
    #except sqlalchemy.exc.OperationalError:
    #    log.logger.warning("修正股票清单表出错，可能已经修正过")
        
        
    
    #事实上在初始化过程中
    #已经将所有表replace掉了
    #事先建表已经失去意义
    #可以考虑在建立完数据后，进行表格的变更
    try:
        #这里应当做一件事，就是根据股票列表
        #建立所有的财务报表、kdata表、adjdata表，以及dailybasic表
        
        sqlStr=''
        
        for stockCode in stockCodeList:
            
            sqlStr='CREATE TABLE `s_finreport_income_%s` (\
    `ts_code` varchar(20) COLLATE utf8mb4_bin,\
      `ann_date` text COLLATE utf8mb4_bin,\
      `f_ann_date` text COLLATE utf8mb4_bin,\
      `end_date` varchar(20) COLLATE utf8mb4_bin,\
      `report_type` text COLLATE utf8mb4_bin,\
      `comp_type` text COLLATE utf8mb4_bin,\
      `basic_eps` double DEFAULT NULL,\
      `diluted_eps` double DEFAULT NULL,\
      `total_revenue` double DEFAULT NULL,\
      `revenue` double DEFAULT NULL,\
      `int_income` text COLLATE utf8mb4_bin,\
      `prem_earned` text COLLATE utf8mb4_bin,\
      `comm_income` text COLLATE utf8mb4_bin,\
      `n_commis_income` text COLLATE utf8mb4_bin,\
      `n_oth_income` text COLLATE utf8mb4_bin,\
      `n_oth_b_income` text COLLATE utf8mb4_bin,\
      `prem_income` text COLLATE utf8mb4_bin,\
      `out_prem` text COLLATE utf8mb4_bin,\
      `une_prem_reser` text COLLATE utf8mb4_bin,\
      `reins_income` text COLLATE utf8mb4_bin,\
      `n_sec_tb_income` text COLLATE utf8mb4_bin,\
      `n_sec_uw_income` text COLLATE utf8mb4_bin,\
      `n_asset_mg_income` text COLLATE utf8mb4_bin,\
      `oth_b_income` text COLLATE utf8mb4_bin,\
      `fv_value_chg_gain` double DEFAULT NULL,\
      `invest_income` double DEFAULT NULL,\
      `ass_invest_income` double DEFAULT NULL,\
      `forex_gain` text COLLATE utf8mb4_bin,\
      `total_cogs` double DEFAULT NULL,\
      `oper_cost` double DEFAULT NULL,\
      `int_exp` text COLLATE utf8mb4_bin,\
      `comm_exp` text COLLATE utf8mb4_bin,\
      `biz_tax_surchg` double DEFAULT NULL,\
      `sell_exp` double DEFAULT NULL,\
      `admin_exp` double DEFAULT NULL,\
      `fin_exp` double DEFAULT NULL,\
      `assets_impair_loss` double DEFAULT NULL,\
      `prem_refund` text COLLATE utf8mb4_bin,\
      `compens_payout` text COLLATE utf8mb4_bin,\
      `reser_insur_liab` text COLLATE utf8mb4_bin,\
      `div_payt` text COLLATE utf8mb4_bin,\
      `reins_exp` text COLLATE utf8mb4_bin,\
      `oper_exp` text COLLATE utf8mb4_bin,\
      `compens_payout_refu` text COLLATE utf8mb4_bin,\
      `insur_reser_refu` text COLLATE utf8mb4_bin,\
      `reins_cost_refund` text COLLATE utf8mb4_bin,\
      `other_bus_cost` text COLLATE utf8mb4_bin,\
      `operate_profit` double DEFAULT NULL,\
      `non_oper_income` double DEFAULT NULL,\
      `non_oper_exp` double DEFAULT NULL,\
      `nca_disploss` double DEFAULT NULL,\
      `total_profit` double DEFAULT NULL,\
      `income_tax` double DEFAULT NULL,\
      `n_income` double DEFAULT NULL,\
      `n_income_attr_p` double DEFAULT NULL,\
      `minority_gain` double DEFAULT NULL,\
      `oth_compr_income` double DEFAULT NULL,\
      `t_compr_income` double DEFAULT NULL,\
      `compr_inc_attr_p` double DEFAULT NULL,\
      `compr_inc_attr_m_s` double DEFAULT NULL,\
      `ebit` double DEFAULT NULL,\
      `ebitda` double DEFAULT NULL,\
      `insurance_exp` text COLLATE utf8mb4_bin,\
      `undist_profit` text COLLATE utf8mb4_bin,\
      `distable_profit` text COLLATE utf8mb4_bin,\
      `update_flag` TINYINT(1) NOT NULL,\
       PRIMARY KEY (`ts_code`,`end_date`,`update_flag`),\
       CONSTRAINT `FK_TC_IC_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list`(`ts_code`)\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;'%(stockCode[:6],stockCode[:6])
    
            #建立表
            mysqlProcessor.execSql(mysqlSession,sqlStr,False)
    
            sqlStr='CREATE TABLE `s_finreport_balancesheet_%s` (\
    `ts_code` varchar(20) COLLATE utf8mb4_bin,\
    `ann_date` text COLLATE utf8mb4_bin,\
    `f_ann_date` text COLLATE utf8mb4_bin,\
    `end_date` varchar(20) COLLATE utf8mb4_bin,\
    `report_type` text COLLATE utf8mb4_bin,\
    `comp_type` text COLLATE utf8mb4_bin,\
    `total_share` double DEFAULT NULL,\
    `cap_rese` double DEFAULT NULL,\
    `undistr_porfit` double DEFAULT NULL,\
    `surplus_rese` double DEFAULT NULL,\
    `special_rese` text COLLATE utf8mb4_bin,\
    `money_cap` double DEFAULT NULL,\
    `trad_asset` double DEFAULT NULL,\
    `notes_receiv` double DEFAULT NULL,\
    `accounts_receiv` double DEFAULT NULL,\
    `oth_receiv` double DEFAULT NULL,\
    `prepayment` double DEFAULT NULL,\
    `div_receiv` text COLLATE utf8mb4_bin,\
    `int_receiv` double DEFAULT NULL,\
    `inventories` double DEFAULT NULL,\
    `amor_exp` double DEFAULT NULL,\
    `nca_within_1y` text COLLATE utf8mb4_bin,\
    `sett_rsrv` text COLLATE utf8mb4_bin,\
    `loanto_oth_bank_fi` double DEFAULT NULL,\
    `premium_receiv` text COLLATE utf8mb4_bin,\
    `reinsur_receiv` text COLLATE utf8mb4_bin,\
    `reinsur_res_receiv` text COLLATE utf8mb4_bin,\
    `pur_resale_fa` double DEFAULT NULL,\
    `oth_cur_assets` text COLLATE utf8mb4_bin,\
    `total_cur_assets` double DEFAULT NULL,\
    `fa_avail_for_sale` double DEFAULT NULL,\
    `htm_invest` double DEFAULT NULL,\
    `lt_eqt_invest` double DEFAULT NULL,\
    `invest_real_estate` double DEFAULT NULL,\
    `time_deposits` text COLLATE utf8mb4_bin,\
    `oth_assets` double DEFAULT NULL,\
    `lt_rec` text COLLATE utf8mb4_bin,\
    `fix_assets` double DEFAULT NULL,\
    `cip` double DEFAULT NULL,\
    `const_materials` text COLLATE utf8mb4_bin,\
    `fixed_assets_disp` double DEFAULT NULL,\
    `produc_bio_assets` text COLLATE utf8mb4_bin,\
    `oil_and_gas_assets` text COLLATE utf8mb4_bin,\
    `intan_assets` double DEFAULT NULL,\
    `r_and_d` text COLLATE utf8mb4_bin,\
    `goodwill` double DEFAULT NULL,\
    `lt_amor_exp` double DEFAULT NULL,\
    `defer_tax_assets` double DEFAULT NULL,\
    `decr_in_disbur` double DEFAULT NULL,\
    `oth_nca` text COLLATE utf8mb4_bin,\
    `total_nca` double DEFAULT NULL,\
    `cash_reser_cb` double DEFAULT NULL,\
    `depos_in_oth_bfi` double DEFAULT NULL,\
    `prec_metals` double DEFAULT NULL,\
    `deriv_assets` double DEFAULT NULL,\
    `rr_reins_une_prem` text COLLATE utf8mb4_bin,\
    `rr_reins_outstd_cla` text COLLATE utf8mb4_bin,\
    `rr_reins_lins_liab` text COLLATE utf8mb4_bin,\
    `rr_reins_lthins_liab` text COLLATE utf8mb4_bin,\
    `refund_depos` text COLLATE utf8mb4_bin,\
    `ph_pledge_loans` text COLLATE utf8mb4_bin,\
    `refund_cap_depos` text COLLATE utf8mb4_bin,\
    `indep_acct_assets` text COLLATE utf8mb4_bin,\
    `client_depos` text COLLATE utf8mb4_bin,\
    `client_prov` text COLLATE utf8mb4_bin,\
    `transac_seat_fee` text COLLATE utf8mb4_bin,\
    `invest_as_receiv` double DEFAULT NULL,\
    `total_assets` double DEFAULT NULL,\
    `lt_borr` double DEFAULT NULL,\
    `st_borr` double DEFAULT NULL,\
    `cb_borr` double DEFAULT NULL,\
    `depos_ib_deposits` text COLLATE utf8mb4_bin,\
    `loan_oth_bank` double DEFAULT NULL,\
    `trading_fl` double DEFAULT NULL,\
    `notes_payable` text COLLATE utf8mb4_bin,\
    `acct_payable` double DEFAULT NULL,\
    `adv_receipts` double DEFAULT NULL,\
    `sold_for_repur_fa` double DEFAULT NULL,\
    `comm_payable` text COLLATE utf8mb4_bin,\
    `payroll_payable` double DEFAULT NULL,\
    `taxes_payable` double DEFAULT NULL,\
    `int_payable` double DEFAULT NULL,\
    `div_payable` double DEFAULT NULL,\
    `oth_payable` double DEFAULT NULL,\
    `acc_exp` double DEFAULT NULL,\
    `deferred_inc` double DEFAULT NULL,\
    `st_bonds_payable` text COLLATE utf8mb4_bin,\
    `payable_to_reinsurer` text COLLATE utf8mb4_bin,\
    `rsrv_insur_cont` text COLLATE utf8mb4_bin,\
    `acting_trading_sec` text COLLATE utf8mb4_bin,\
    `acting_uw_sec` text COLLATE utf8mb4_bin,\
    `non_cur_liab_due_1y` double DEFAULT NULL,\
    `oth_cur_liab` double DEFAULT NULL,\
    `total_cur_liab` double DEFAULT NULL,\
    `bond_payable` double DEFAULT NULL,\
    `lt_payable` double DEFAULT NULL,\
    `specific_payables` text COLLATE utf8mb4_bin,\
    `estimated_liab` double DEFAULT NULL,\
    `defer_tax_liab` double DEFAULT NULL,\
    `defer_inc_non_cur_liab` text COLLATE utf8mb4_bin,\
    `oth_ncl` text COLLATE utf8mb4_bin,\
    `total_ncl` double DEFAULT NULL,\
    `depos_oth_bfi` double DEFAULT NULL,\
    `deriv_liab` double DEFAULT NULL,\
    `depos` double DEFAULT NULL,\
    `agency_bus_liab` double DEFAULT NULL,\
    `oth_liab` double DEFAULT NULL,\
    `prem_receiv_adva` text COLLATE utf8mb4_bin,\
    `depos_received` text COLLATE utf8mb4_bin,\
    `ph_invest` text COLLATE utf8mb4_bin,\
    `reser_une_prem` text COLLATE utf8mb4_bin,\
    `reser_outstd_claims` text COLLATE utf8mb4_bin,\
    `reser_lins_liab` text COLLATE utf8mb4_bin,\
    `reser_lthins_liab` text COLLATE utf8mb4_bin,\
    `indept_acc_liab` text COLLATE utf8mb4_bin,\
    `pledge_borr` text COLLATE utf8mb4_bin,\
    `indem_payable` text COLLATE utf8mb4_bin,\
    `policy_div_payable` text COLLATE utf8mb4_bin,\
    `total_liab` double DEFAULT NULL,\
    `treasury_share` text COLLATE utf8mb4_bin,\
    `ordin_risk_reser` double DEFAULT NULL,\
    `forex_differ` double DEFAULT NULL,\
    `invest_loss_unconf` text COLLATE utf8mb4_bin,\
    `minority_int` double DEFAULT NULL,\
    `total_hldr_eqy_exc_min_int` double DEFAULT NULL,\
    `total_hldr_eqy_inc_min_int` double DEFAULT NULL,\
    `total_liab_hldr_eqy` double DEFAULT NULL,\
    `lt_payroll_payable` text COLLATE utf8mb4_bin,\
    `oth_comp_income` double DEFAULT NULL,\
    `oth_eqt_tools` double DEFAULT NULL,\
    `oth_eqt_tools_p_shr` double DEFAULT NULL,\
    `lending_funds` text COLLATE utf8mb4_bin,\
    `acc_receivable` text COLLATE utf8mb4_bin,\
    `st_fin_payable` text COLLATE utf8mb4_bin,\
    `payables` text COLLATE utf8mb4_bin,\
    `hfs_assets` text COLLATE utf8mb4_bin,\
    `hfs_sales` text COLLATE utf8mb4_bin,\
    `update_flag` TINYINT(1) NOT NULL,\
     PRIMARY KEY (`ts_code`,`end_date`,`update_flag`),\
     CONSTRAINT `FK_TC_BS_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`)\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;'%(stockCode[:6],stockCode[:6])
            #建立表
            mysqlProcessor.execSql(mysqlSession,sqlStr,False)
        
            sqlStr='CREATE TABLE `s_finreport_cashflow_%s` (\
      `ts_code` varchar(20) COLLATE utf8mb4_bin,\
      `ann_date` text COLLATE utf8mb4_bin,\
      `f_ann_date` text COLLATE utf8mb4_bin,\
      `end_date` varchar(20) COLLATE utf8mb4_bin,\
      `comp_type` text COLLATE utf8mb4_bin,\
      `report_type` text COLLATE utf8mb4_bin,\
      `net_profit` double DEFAULT NULL,\
      `finan_exp` double DEFAULT NULL,\
      `c_fr_sale_sg` double DEFAULT NULL,\
      `recp_tax_rends` double DEFAULT NULL,\
      `n_depos_incr_fi` text COLLATE utf8mb4_bin,\
      `n_incr_loans_cb` text COLLATE utf8mb4_bin,\
      `n_inc_borr_oth_fi` text COLLATE utf8mb4_bin,\
      `prem_fr_orig_contr` text COLLATE utf8mb4_bin,\
      `n_incr_insured_dep` text COLLATE utf8mb4_bin,\
      `n_reinsur_prem` text COLLATE utf8mb4_bin,\
      `n_incr_disp_tfa` text COLLATE utf8mb4_bin,\
      `ifc_cash_incr` text COLLATE utf8mb4_bin,\
      `n_incr_disp_faas` text COLLATE utf8mb4_bin,\
      `n_incr_loans_oth_bank` text COLLATE utf8mb4_bin,\
      `n_cap_incr_repur` text COLLATE utf8mb4_bin,\
      `c_fr_oth_operate_a` double DEFAULT NULL,\
      `c_inf_fr_operate_a` double DEFAULT NULL,\
      `c_paid_goods_s` double DEFAULT NULL,\
      `c_paid_to_for_empl` double DEFAULT NULL,\
      `c_paid_for_taxes` double DEFAULT NULL,\
      `n_incr_clt_loan_adv` text COLLATE utf8mb4_bin,\
      `n_incr_dep_cbob` text COLLATE utf8mb4_bin,\
      `c_pay_claims_orig_inco` text COLLATE utf8mb4_bin,\
      `pay_handling_chrg` text COLLATE utf8mb4_bin,\
      `pay_comm_insur_plcy` text COLLATE utf8mb4_bin,\
      `oth_cash_pay_oper_act` double DEFAULT NULL,\
      `st_cash_out_act` double DEFAULT NULL,\
      `n_cashflow_act` double DEFAULT NULL,\
      `oth_recp_ral_inv_act` double DEFAULT NULL,\
      `c_disp_withdrwl_invest` double DEFAULT NULL,\
      `c_recp_return_invest` double DEFAULT NULL,\
      `n_recp_disp_fiolta` double DEFAULT NULL,\
      `n_recp_disp_sobu` double DEFAULT NULL,\
      `stot_inflows_inv_act` double DEFAULT NULL,\
      `c_pay_acq_const_fiolta` double DEFAULT NULL,\
      `c_paid_invest` double DEFAULT NULL,\
      `n_disp_subs_oth_biz` double DEFAULT NULL,\
      `oth_pay_ral_inv_act` double DEFAULT NULL,\
      `n_incr_pledge_loan` double DEFAULT NULL,\
      `stot_out_inv_act` double DEFAULT NULL,\
      `n_cashflow_inv_act` double DEFAULT NULL,\
      `c_recp_borrow` double DEFAULT NULL,\
      `proc_issue_bonds` double DEFAULT NULL,\
      `oth_cash_recp_ral_fnc_act` double DEFAULT NULL,\
      `stot_cash_in_fnc_act` double DEFAULT NULL,\
      `free_cashflow` double DEFAULT NULL,\
      `c_prepay_amt_borr` double DEFAULT NULL,\
      `c_pay_dist_dpcp_int_exp` double DEFAULT NULL,\
      `incl_dvd_profit_paid_sc_ms` text COLLATE utf8mb4_bin,\
      `oth_cashpay_ral_fnc_act` double DEFAULT NULL,\
      `stot_cashout_fnc_act` double DEFAULT NULL,\
      `n_cash_flows_fnc_act` double DEFAULT NULL,\
      `eff_fx_flu_cash` double DEFAULT NULL,\
      `n_incr_cash_cash_equ` double DEFAULT NULL,\
      `c_cash_equ_beg_period` double DEFAULT NULL,\
      `c_cash_equ_end_period` double DEFAULT NULL,\
      `c_recp_cap_contrib` double DEFAULT NULL,\
      `incl_cash_rec_saims` double DEFAULT NULL,\
      `uncon_invest_loss` text COLLATE utf8mb4_bin,\
      `prov_depr_assets` double DEFAULT NULL,\
      `depr_fa_coga_dpba` double DEFAULT NULL,\
      `amort_intang_assets` double DEFAULT NULL,\
      `lt_amort_deferred_exp` double DEFAULT NULL,\
      `decr_deferred_exp` double DEFAULT NULL,\
      `incr_acc_exp` double DEFAULT NULL,\
      `loss_disp_fiolta` double DEFAULT NULL,\
      `loss_scr_fa` double DEFAULT NULL,\
      `loss_fv_chg` double DEFAULT NULL,\
      `invest_loss` double DEFAULT NULL,\
      `decr_def_inc_tax_assets` double DEFAULT NULL,\
      `incr_def_inc_tax_liab` double DEFAULT NULL,\
      `decr_inventories` double DEFAULT NULL,\
      `decr_oper_payable` double DEFAULT NULL,\
      `incr_oper_payable` double DEFAULT NULL,\
      `others` double DEFAULT NULL,\
      `im_net_cashflow_oper_act` double DEFAULT NULL,\
      `conv_debt_into_cap` text COLLATE utf8mb4_bin,\
      `conv_copbonds_due_within_1y` text COLLATE utf8mb4_bin,\
      `fa_fnc_leases` double DEFAULT NULL,\
      `end_bal_cash` double DEFAULT NULL,\
      `beg_bal_cash` double DEFAULT NULL,\
      `end_bal_cash_equ` text COLLATE utf8mb4_bin,\
      `beg_bal_cash_equ` text COLLATE utf8mb4_bin,\
      `im_n_incr_cash_equ` double DEFAULT NULL,\
      `update_flag` TINYINT(1) NOT NULL,\
       PRIMARY KEY (`ts_code`,`end_date`,`update_flag`),\
       CONSTRAINT `FK_TC_CF_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`)\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;' %(stockCode[:6],stockCode[:6])
            #建立表
            mysqlProcessor.execSql(mysqlSession,sqlStr,False)    
    
    
            sqlStr='CREATE TABLE `s_kdata_%s` (\
    `ts_code` varchar(20) COLLATE utf8mb4_bin,\
    `trade_date` varchar(20) COLLATE utf8mb4_bin,\
    `open` double DEFAULT NULL,\
    `high` double DEFAULT NULL,\
    `low` double DEFAULT NULL,\
    `close` double DEFAULT NULL,\
    `pre_close` double DEFAULT NULL,\
    `change` double DEFAULT NULL,\
    `pct_chg` double DEFAULT NULL,\
    `vol` double DEFAULT NULL,\
    `amount` double DEFAULT NULL,\
     PRIMARY KEY (`ts_code`,`trade_date`),\
     CONSTRAINT `FK_TC_KD_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`)\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;'%(stockCode[:6],stockCode[:6])
            #建立表
            mysqlProcessor.execSql(mysqlSession,sqlStr,False)  
    
    
            sqlStr='CREATE TABLE `s_dailybasic_%s` (\
      `ts_code` varchar(20) COLLATE utf8mb4_bin,\
      `trade_date` varchar(20) COLLATE utf8mb4_bin,\
      `close` double DEFAULT NULL,\
      `turnover_rate` double DEFAULT NULL,\
      `turnover_rate_f` double DEFAULT NULL,\
      `volume_ratio` double DEFAULT NULL,\
      `pe` double DEFAULT NULL,\
      `pe_ttm` double DEFAULT NULL,\
      `pb` double DEFAULT NULL,\
      `ps` double DEFAULT NULL,\
      `ps_ttm` double DEFAULT NULL,\
      `dv_ratio` double DEFAULT NULL,\
      `dv_ttm` double DEFAULT NULL,\
      `total_share` double DEFAULT NULL,\
      `float_share` double DEFAULT NULL,\
      `free_share` double DEFAULT NULL,\
      `total_mv` double DEFAULT NULL,\
      `circ_mv` double DEFAULT NULL,\
       PRIMARY KEY (`ts_code`,`trade_date`),\
       CONSTRAINT `FK_TC_DB_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`)\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;'%(stockCode[:6],stockCode[:6])
            #建立表
            mysqlProcessor.execSql(mysqlSession,sqlStr,False)  
            
            sqlStr='CREATE TABLE `s_adjdata_%s` (\
    `ts_code` varchar(20) COLLATE utf8mb4_bin,\
    `trade_date` varchar(20) COLLATE utf8mb4_bin,\
    `adj_factor` double DEFAULT NULL,\
     PRIMARY KEY (`ts_code`,`trade_date`),\
     CONSTRAINT `FK_TC_ADJ_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`)\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;'%(stockCode[:6],stockCode[:6])
            #建立表
            mysqlProcessor.execSql(mysqlSession,sqlStr,False)     
            log.logger.info('建立完股票%s的所有表'%(stockCode[:6]))
    except sqlalchemy.exc.OperationalError:
        log.logger.warning("为股票%s建表时出现异常"%(stockCode[:6]))
    
    
    sdProcessor=StockDataProcessor()
    
    # 创建命令行解析器句柄，并自定义描述信息
    parser = argparse.ArgumentParser(description="test the argparse package")
    # 定义必选参数 positionArg
    # parser.add_argument("project_name")
    # 定义可选参数module
    #开始日期为19900101，即取全量数据
    parser.add_argument("--startdate","-sd",type=str, default='19911219',help="Enter the start date")
    # 定义可选参数module1
    #结束日期默认为当前日期的前一个交易日（不含当天，以便解决当天可能还未完成交易的问题）
    parser.add_argument("--enddate","-ed",type=str, default=sdProcessor.getLastDealDay(dt.now().strftime('%Y%m%d'),False),help="Enter the end date")
    # 指定参数类型（默认是 str）
    # parser.add_argument('x', type=int, help='test the type')
    # 设置参数的可选范围
    # parser.add_argument('--verbosity3', '-v3', type=str, choices=['one', 'two', 'three', 'four'], help='test choices')
    # 设置参数默认值
    # parser.add_argument('--verbosity4', '-v4', type=str, choices=['one', 'two', 'three'], default=1,help='test default value')
    args = parser.parse_args()  # 返回一个命名空间
    #print(args)
    params = vars(args)  # 返回 args 的属性和属性值的字典
    v1=[]
    
    sd=''
    ed=''
    
    for k, v in params.items():
        if k=='startdate':
            sd=v
        elif k=='enddate':
            if v>=dt.now().strftime('%Y%m%d'):
                #调整一下日期，如果输入的结束日期等于或晚于当天
                #则取当天之前的一个交易日
                v=StockDataProcessor.getLastDealDay(dt.now().strftime('%Y%m%d'),False)
            ed=v
    
    startday=sd
    endday=ed
    
    
    
    #调整代码逻辑，按照股票来进行更新
    #每次更新一个股票
    #这样可以避免限次数问题
    
    
    
    '''
    #找到之前处理的最后一个股票的代码
    sql="select content from u_data_desc where content_name='finance_report_stockcode_update_to'"
    res=mysqlProcessor.querySql(sql)
    try:
        finance_report_stockcode_update_to=res.at[0,'content']
    except:
        finance_report_stockcode_update_to=''
    
    sql="select content from u_data_desc where content_name='kdata_stockcode_update_to'"
    res=mysqlProcessor.querySql(sql)
    try:
        kdata_stockcode_update_to=res.at[0,'content']
    except:
        kdata_stockcode_update_to=''
    
    sql="select content from u_data_desc where content_name='adjdata_stockcode_update_to'"
    res=mysqlProcessor.querySql(sql)
    try:
        adjdata_stockcode_update_to=res.at[0,'content']
    except:
        adjdata_stockcode_update_to=''
    
    
    stockcode_update_to=min(finance_report_stockcode_update_to,kdata_stockcode_update_to,adjdata_stockcode_update_to)
    
    '''
    
    slList=get8slListFromStockList(stockCodeList)
    

    #manager=Manager()
    
    #分8个进程，分别计算8段股票波动率
    process=[]

    for subStockList in slList:
        
        p=multiprocessing.Process(target=processStockDataGet,args=(subStockList,startday,endday))
        p.start()
        process.append(p)
    
    for p in process:
        p.join()           
                
    
    

       
    #完成所有数据的更新
    totalUpdate(mysqlSession,sdProcessor)
    
    
    #完成所有数据更新，把数据库表重置，以便下一次处理使用
    #lastDataUpdate(mysqlSession,"","FR_StockCode")
    #lastDataUpdate(mysqlSession,"","KD")
    #lastDataUpdate(mysqlSession,"","ADJ")
    #lastDataUpdate(mysqlSession,"","DB")
        
    mysqlSession.close()
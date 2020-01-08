'''
Created on 2019年11月2日

@author: xiaoda
将数据库中的数据更新到最新日期
从数据库中记录的上次更新日，更新到当前日期的前一个交易日
不获取到当前日期，因为可能出现未更新当天数据的情况
'''

import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]

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

import time
import datetime
import tushare
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
#from com.xiaoda.stock.loopbacktester.utils.ParamUtils import LOGGINGDIR
from datetime import datetime as dt
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor

mysqlProcessor=MysqlProcessor()
sdProcessor=StockDataProcessor()

DAYONE='19911219'

log=Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')

mysqlProcessor=MysqlProcessor()

def getlastquarterfirstday():
        today=dt.now()
        quarter=(today.month-1)/3+1
        if quarter==1:
            return dt(today.year-1,10,1)
        elif quarter==2:
            return dt(today.year,1,1)
        elif quarter==3:
            return dt(today.year,4,1)
        else:
            return dt(today.year,7,1)
    



def partialUpdate(mysqlSession):    
    #部分更新语句
    pupdatesql="update u_data_desc set content='%s' where content_name='last_update_time';"%(dt.now().strftime('%Y%m%d %H:%M:%S'))
    MysqlProcessor.execSql(mysqlSession,pupdatesql,True)

def totalUpdate(mysqlSession,endday):
    #全局更新语句
    tupdatesql="update u_data_desc set content='%s' where content_name='last_total_update_time';"%(dt.now().strftime('%Y%m%d %H:%M:%S'))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    #不更新起始日期
    #tupdatesql="update u_data_desc set content='%s' where content_name='data_start_date';"%(sd)
    #MysqlProcessor.execSql(mysqlSession,tupdatesql,True)
    #只更新结束日期为当天的前一个交易日
    tupdatesql="update u_data_desc set content='%s' where content_name='data_end_dealday';"%(endday)
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    mysqlSession.commit()


def lastDataUpdate(mysqlSession,stockCode,dataType):
    if dataType=='FR_StockCode':
        #更新财务报表最新股票代码
        sql="update u_data_desc set content='%s' where content_name='finance_report_stockcode_update_to';"%(stockCode)
    elif dataType=='FR_Date':
        sql="update u_data_desc set content='%s' where content_name='finance_report_date_update_to';"%(endday)
    elif dataType=="KD":
        #更新K线最新股票代码
        sql="update u_data_desc set content='%s' where content_name='kdata_update_to';"%(stockCode)
    elif dataType=="ADJ":
        #更新复权因子最新股票代码
        sql="update u_data_desc set content='%s' where content_name='adjdata_update_to';"%(stockCode)
    else:
        raise Exception("dataType不正确")
    
    MysqlProcessor.execSql(mysqlSession,sql,True)
        

#使用TuShare pro版本
#初始化tushare账号
tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
sdDataAPI=tushare.pro_api()

#写入数据库的引擎
mysqlEngine=mysqlProcessor.getMysqlEngine()
mysqlSession=mysqlProcessor.getMysqlSession()





startday=DAYONE

#1、获取交易日信息，并存入数据库

#交易日历数据相对简单，可以每次都全量获取
#需要先获取完整的交易日历，以方便后续对日期的处理和判断
trade_cal_data=sdDataAPI.trade_cal(exchange='SSE',start_date=startday,end_date=dt.now().strftime('%Y%m%d'),\
                                   fields='exchange,cal_date,is_open,pretrade_date')

#将交易日列表存入数据库表中
trade_cal_data.to_sql(name='u_trade_cal',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)

log.logger.info('处理完交易日数据的更新')
#完成部分信息更新
partialUpdate(mysqlSession)




#查询语句
#查询当前数据截止的日期
sql = "select content from u_data_desc where content_name='data_end_dealday'"
res=mysqlProcessor.querySql(sql)
#如果已经有数据了，那么设置本次数据更新起始时间
#为现有数据最后日期的下一天
if not res.empty:
    last_endday=dt.strptime(res.at[0,'content'], "%Y%m%d").date()
    startday=(last_endday+datetime.timedelta(1)).strftime('%Y%m%d')
else:
    #数据库中无数
    #不应当进行更新
    sys.exit(0)


#日期是最新的，在日历表中没有数据，需要先获取日历数据后再进行判断
endday=sdProcessor.getLastDealDay(dt.now().strftime('%Y%m%d'),False)





#如果前一个交易日已经更新过，直接退出
if last_endday.strftime('%Y%m%d')>=endday:
    sys.exit(0)



#3、获取指数信息
indexDF=sdDataAPI.index_daily(ts_code='000300.SH',start_date=startday,end_date=endday)

#将指数数据存入数据库表中
indexDF.to_sql(name='u_idx_hs300',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)





#找到之前最后一次处理财务报表的时间
sql="select content from u_data_desc where content_name='finance_report_date_update_to'"
res=mysqlProcessor.querySql(sql)
finance_report_date_update_to=res.at[0,'content']

finReportDis=StockDataProcessor.getDateDistance(finance_report_date_update_to,dt.now().strftime('%Y%m%d'))

#标记是否需要处理财务报表
proFinRepFlg=False

#只有在上次更新日期在1个月以上时才进行更新财务报表数据
if finReportDis>=30:
    proFinRepFlg=True






#2、获取股票列表并存入数据库
#股票列表数据相对简单，可以每次都全量获取

stockListDF=sdDataAPI.stock_basic(exchange='',list_status='L',fields='ts_code,symbol,name,area,industry,list_date')

stockCodeList=list(stockListDF['ts_code'])

#stockListDF.set_index('ts_code',drop=True,inplace=True)
#对股票清单里面的股票，需要一个一个过
#如果是已经存在在数据库里，则不用建表，直接向股票清单表及其他相关表插入数据即可
#如果是还不存在数据库里面的，需要为这个股票新建各种表，然后插入数据
#整体框架应当是以股票为单位进行循环





ix=0
length=len(stockCodeList)

while ix<length:
   
   
    try:
        
        stockCode=stockCodeList[ix]
    
        cnt=mysqlProcessor.querySql("select count(*) as count from u_stock_list where ts_code='%s'"%(stockCode))
    
        if cnt.at[0,'count']>0:
        
            #ix+=1
            #continue
            
            #在股票清单中已存在
            #不需要在股票清单表中进行处理
            
            #插入财务数据表
            #只有当股票的财务报表超过1个月未更新时才进行处理
            if proFinRepFlg==True:
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
                bs.to_sql(name='s_finreport_balancesheet_'+stockCode[:6],con=mysqlEngine,chunksize=100,if_exists='append',index=None)
                  
                #获取现金流量表
                cf=sdDataAPI.cashflow(ts_code=stockCode,start_date=startday,end_date=endday,fields=
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
                cf.to_sql(name='s_finreport_cashflow_'+stockCode[:6],con=mysqlEngine,chunksize=100,if_exists='append',index=None)
                
                #获取利润表
                ic=sdDataAPI.income(ts_code=stockCode,start_date=startday,end_date=endday,fields=
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
                ic.to_sql(name='s_finreport_income_'+stockCode[:6],con=mysqlEngine,chunksize=100,if_exists='append',index=None)
    
            #插入adjdata表
            adj_data=sdDataAPI.adj_factor(ts_code=stockCode,start_date=startday,end_date=endday)
            adj_data.sort_index(inplace=True,ascending=False) 
            #存入数据库
            adj_data.to_sql(name='s_adjdata_'+stockCode[:6],con=mysqlEngine, chunksize=1000,if_exists='append', index=None)        
          
                          
            #插入kdata表、dailybasic表
            #kdataFlg=False
            #dbFlg=False
            
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
                
                #获取该股票KDATA数据并写入数据库
                stock_k_data=tushare.pro_bar(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if stock_k_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE))   
                    #要注意一个问题，如果是为空，如果直接跳出，会导致下一次如果在本时段没有交易的股票，没有replace的过程
                    #会重复添加到数据库表，按理说如果是空，在这个过程中应当是先创建一个空表才对
                else:
                    #该股票出现交易数据，必然是第一次出现，直接替代即可
                    #kdataFlg=True
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
                
                
                #获取该股票DB数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                else:
                    #dbFlg=True
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
                
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
                else:
                    #该股票出现交易数据
                    #且在上一区间已经有过交易，直接增加数据即可
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
    
    
                #获取该股票数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                else:
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
    
            
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
                else:
                    #该股票出现交易数据
                    #且在上一区间已经有过交易，直接增加数据即可
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
    
                #获取该股票数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                else:
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
    
            
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
                else:
                    #该股票出现交易数据
                    #且在上一区间已经有过交易，直接增加数据即可
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)               
    
                #获取该股票数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                else:
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None) 
                  
        else:
            #在股票清单中不存在
            #需要建立股票相关的各个表
            
            #先把股票信息插入股票清单表
            valStr=stockListDF[ix:ix+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
            valList=valStr.split(',')
            paramStr=''
            for val in valList:
                if val=='NULL':
                    paramStr=paramStr+'NULL,'
                else:       
                    paramStr=paramStr+"'"+val+"'"+','
            paramStr=paramStr[:-1]
            sql='insert into u_stock_list(ts_code,symbol,name,area,industry,list_date) values (%s)'%(paramStr)
            MysqlProcessor.execSql(mysqlSession,sql,True)       
    
    
            #写入财务报表，并对表进行修正
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
            bs.to_sql(name='s_finreport_balancesheet_'+stockCode[:6],con=mysqlEngine,chunksize=100,if_exists='replace',index=None)

            sql='alter table s_finreport_balancesheet_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)           
            sql='alter table s_finreport_balancesheet_%s MODIFY COLUMN end_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)  
            sql='alter table s_finreport_balancesheet_%s MODIFY COLUMN update_flag TINYINT(1) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)              
            sql='alter table s_finreport_balancesheet_%s add PRIMARY KEY (`ts_code`,`end_date`,`update_flag`);'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)  
            sql='alter table s_finreport_balancesheet_%s add CONSTRAINT `FK_BS_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)              

              
            #获取现金流量表
            cf=sdDataAPI.cashflow(ts_code=stockCode,start_date=startday,end_date=endday,fields=
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
            cf.to_sql(name='s_finreport_cashflow_'+stockCode[:6],con=mysqlEngine,chunksize=100,if_exists='replace',index=None)

            sql='alter table s_finreport_cashflow_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)
            sql='alter table s_finreport_cashflow_%s MODIFY COLUMN end_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)
            sql='alter table s_finreport_cashflow_%s MODIFY COLUMN update_flag TINYINT(1) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)
            sql='alter table s_finreport_cashflow_%s add PRIMARY KEY (`ts_code`,`end_date`,`update_flag`);'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)
            sql='alter table s_finreport_cashflow_%s add CONSTRAINT `FK_CF_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)
            
            #获取利润表
            ic=sdDataAPI.income(ts_code=stockCode,start_date=startday,end_date=endday,fields=
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
            ic.to_sql(name='s_finreport_income_'+stockCode[:6],con=mysqlEngine,chunksize=100,if_exists='replace',index=None)
            
            #修正表结构，增加索引、主键等
            sql='alter table s_finreport_income_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)           
            sql='alter table s_finreport_income_%s MODIFY COLUMN end_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)  
            sql='alter table s_finreport_income_%s MODIFY COLUMN update_flag TINYINT(1) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)              
            sql='alter table s_finreport_income_%s add PRIMARY KEY (`ts_code`,`end_date`,`update_flag`);'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)          
            sql='alter table s_finreport_income_%s add CONSTRAINT `FK_IC_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)
                        
                                        
            #插入adjdata表
            adj_data=sdDataAPI.adj_factor(ts_code=stockCode,start_date=startday,end_date=endday)
            adj_data.sort_index(inplace=True,ascending=False) 
            #存入数据库
            adj_data.to_sql(name='s_adjdata_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='replace',index=None) 
    
            #修正表结构，增加索引、主键等
            sql='alter table s_adjdata_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)
            sql='alter table s_adjdata_%s MODIFY COLUMN trade_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)
            sql='alter table s_adjdata_%s add PRIMARY KEY (`ts_code`,`trade_date`);'%(stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)
            sql='alter table s_adjdata_%s add CONSTRAINT `FK_AJD_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
            MysqlProcessor.execSql(mysqlSession,sql,True)                                      
                             
            #插入kdata表、dailybasic表
            #并修正表结构，增加索引、主键等
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
                
                #获取该股票KDATA数据并写入数据库
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
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
                    
                    sql='alter table s_kdata_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s MODIFY COLUMN trade_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s add PRIMARY KEY (`ts_code`,`trade_date`);'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s add CONSTRAINT `FK_KD_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                
                
                #获取该股票DB数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                else:
                    dbFlg=True
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
                    
                    sql='alter table s_dailybasic_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s MODIFY COLUMN trade_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s add PRIMARY KEY (`ts_code`,`trade_date`);'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s add CONSTRAINT `FK_DB_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                
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
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)                                  
                    
                    sql='alter table s_kdata_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s MODIFY COLUMN trade_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s add PRIMARY KEY (`ts_code`,`trade_date`);'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s add CONSTRAINT `FK_KD_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                else:
                    #该股票出现交易数据
                    #且在上一区间已经有过交易，直接增加数据即可
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
    
    
                #获取该股票数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                elif dbFlg==False:
                    dbFlg=True
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
                    
                    sql='alter table s_dailybasic_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s MODIFY COLUMN trade_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s add PRIMARY KEY (`ts_code`,`trade_date`);'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s add CONSTRAINT `FK_DB_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)                    
                else:
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
    
            
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
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)                    
                    
                    sql='alter table s_kdata_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s MODIFY COLUMN trade_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s add PRIMARY KEY (`ts_code`,`trade_date`);'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s add CONSTRAINT `FK_KD_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                else:
                    #该股票出现交易数据
                    #且在上一区间已经有过交易，直接增加数据即可
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
    
                #获取该股票数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                elif dbFlg==False:
                    dbFlg=True
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
                    
                    sql='alter table s_dailybasic_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s MODIFY COLUMN trade_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s add PRIMARY KEY (`ts_code`,`trade_date`);'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s add CONSTRAINT `FK_DB_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                else:
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
    
            
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
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)                      
                    
                    sql='alter table s_kdata_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s MODIFY COLUMN trade_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s add PRIMARY KEY (`ts_code`,`trade_date`);'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_kdata_%s add CONSTRAINT `FK_KD_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                else:
                    #该股票出现交易数据
                    #且在上一区间已经有过交易，直接增加数据即可
                    stock_k_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    stock_k_data.to_sql(name='s_kdata_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)               
    
                #获取该股票数据并写入数据库
                daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
                if daily_basic_data.empty:
                    #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                    log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
                elif dbFlg==False:
                    dbFlg=True
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    
                    sql='alter table s_dailybasic_%s MODIFY COLUMN ts_code VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s MODIFY COLUMN trade_date VARCHAR(20) COLLATE utf8mb4_bin;'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s add PRIMARY KEY (`ts_code`,`trade_date`);'%(stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                    sql='alter table s_dailybasic_%s add CONSTRAINT `FK_DB_%s` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`);'%(stockCode[:6],stockCode[:6]) 
                    MysqlProcessor.execSql(mysqlSession,sql,True)
                else:
                    daily_basic_data.sort_index(inplace=True,ascending=False)
                    #存入数据库
                    daily_basic_data.to_sql(name='s_dailybasic_'+stockCode[:6],con=mysqlEngine,chunksize=1000,if_exists='append',index=None)           
    except Exception as e:
        log.logger.warning("股票%s的数据更新异常，需要重新处理"%(stockCode))
        #数据更新异常，需要继续重新更新
    else:
        ix=ix+1

#完成所有数据的更新
totalUpdate(mysqlSession,endday)
mysqlSession.close()
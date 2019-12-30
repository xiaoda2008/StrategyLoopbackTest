'''
Created on 2019年11月2日

@author: xiaoda
将数据库中的数据更新到最新日期
从数据库中记录的上次更新日，更新到当前日期的前一个交易日
不获取到当前日期，因为可能出现未更新当天数据的情况
'''

import sys
import os
from builtins import False
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]

#print(sys.path)
#sys.path.clear()
print(sys.path)
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

def totalUpdate(mysqlSession):
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
trade_cal_data=sdDataAPI.trade_cal(exchange='',start_date=startday,end_date=dt.now().strftime('%Y%m%d'),fields='exchange,cal_date,is_open,pretrade_date')

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
if last_endday>=endday:
    sys.exit(0)



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
   
    stockCode=stockCodeList[ix]

    cnt=mysqlProcessor.querySql("select count(*) as count from u_stock_list where ts_code='%s'"%(stockCode))
    if cnt.at[0,'count']>0:
        #已存在
        #插入财务数据表
        if proFinRepFlg==True:
            
        #插入kdata表
        
        #插入dailybasic表
        
        #插入adjdata表
        
        pass
        
        
        
    else:
        #表不存在
        #插入股票清单表
        valStr=stockListDF[ix:ix+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
        valList=valStr.split(',')
        paramStr=''
        for val in valList:
            if val=='NULL':
                paramStr=paramStr+'NULL,'
            else:       
                paramStr=paramStr+"'"+val+"'"+','
        paramStr=paramStr[:-1]
        sql='insert into u_stock_list values (%s)'%(paramStr)
        MysqlProcessor.execSql(mysqlSession,sql,True)       
        
        

        
        #插入财务数据表
        #if proFinRepFlg==True:

            #写入财务报表，并对表格进行修正
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
                      con=mysqlEngine,chunksize=100,if_exists='append',index=None)
            
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
                      con=mysqlEngine,chunksize=100,if_exists='append',index=None)
            
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
                      con=mysqlEngine,chunksize=100,if_exists='append',index=None)
            
                        变更表结构，增加索引、主键等
                         
        #插入kdata表
        
                    变更表结构，增加索引、主键等
                    
        #插入dailybasic表
        
                    变更表结构，增加索引、主键等
                    
        #插入adjdata表
            
                变更表结构，增加索引、主键等
            


#3、获取财务报表数据，存入数据库

#查询语句
#查询当前数据截止的日期
sql="select content from u_data_desc where content_name='data_end_dealday'"
res=mysqlProcessor.querySql(sql)


#如果已经有数据了，那么设置本次数据更新起始时间
#为现有数据最后日期的下一天
if not res.empty:
    cday=dt.strptime(res.at[0,'content'], "%Y%m%d").date()
    startday=(cday+datetime.timedelta(1)).strftime('%Y%m%d')


#找到之前处理的最后一个股票的代码
sql="select content from u_data_desc where content_name='finance_report_stockcode_update_to'"
res=mysqlProcessor.querySql(sql)
finance_report_stockcode_update_to=res.at[0,'content']

#找到之前最后一次处理财务报表的时间
sql="select content from u_data_desc where content_name='finance_report_date_update_to'"
res=mysqlProcessor.querySql(sql)
finance_report_date_update_to=res.at[0,'content']

dis=StockDataProcessor.getDateDistance(finance_report_date_update_to,dt.now().strftime('%Y%m%d'))


#只有在上次更新日期在1个月以上时才进行更新财务报表数据
if dis>=30:
    for index,stockCode in stockCodeList.items():
    
        if stockCode<=finance_report_stockcode_update_to:
            continue
        
    #    if endday-startday<一季度:
    #        log.logger.info('%s到%s时间段不足一季度，跳过取财务报表环节'%(stockCode,startday,endday))
    #        continue
    
        #获取资产负债表
        bs=sdDataAPI.balancesheet(ts_code=stockCode,start_date=startday,end_date=endday)
        #获取现金流量表
        cf=sdDataAPI.cashflow(ts_code=stockCode,start_date=startday,end_date=endday)
        #获取利润表
        ic=sdDataAPI.income(ts_code=stockCode,start_date=startday,end_date=endday)
        
        if bs.empty or cf.empty or ic.empty:
            log.logger.info('%s在%s到%s时间段没有发布新的财务报表'%(stockCode,startday,endday))
            #虽然没有发布新的财务报表
            #但对于本程序来说，已完成处理任务
            #该股票数据处理完毕
            partialUpdate(mysqlSession)
            lastDataUpdate(mysqlSession,stockCode,"FR")
            time.sleep(0.75)
            continue
        
        #由于是更新数据，绝大多数的表在此前都已经存在
        #因此，不能直接用dataframe的to_sql写入，否则会删除原有数据，或者可能重复插入
        #因此，需要生成sql代码进行插入
        
        #但对于确实没有数据的股票：刚刚上市，或者之前还没有发不过去财务报表等的，应当还是先用to_sql建立表格
        
        sql="select table_name from information_schema.tables where table_name='s_balancesheet_%s'"%(stockCode[:6])
        res=mysqlProcessor.querySql(sql)
        #如果数据库中还没有这个表，需要建立表格
        if res.empty:
            bs.to_sql(name='s_balancesheet_'+stockCode[:6],
                      con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
        else:
            #数据库中已经有这个表了
            #对balancesheet表中的数据，生成sql插入语句
            for bidx in bs.index:
                valStr=bs[bidx:bidx+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
                valList=valStr.split(',')
                paramStr=''
                for val in valList:
                    if val=='NULL':
                        paramStr=paramStr+'NULL,'
                    else:       
                        paramStr=paramStr+"'"+val+"'"+','
                paramStr=paramStr[:-1]
                sql='insert into s_balancesheet_%s values (%s)'%(stockCode[:6],paramStr)
                MysqlProcessor.execSql(mysqlSession,sql,False)
    
        sql="select table_name from information_schema.tables where table_name='s_cashflow_%s'"%(stockCode[:6])
        res=mysqlProcessor.querySql(sql)
        #如果数据库中还没有这个表，需要建立表格
        if res.empty:    
            cf.to_sql(name='s_cashflow_'+stockCode[:6],
                      con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
        else:
            #数据库中已经有这个表了
            #对cashflow表中的数据，生成sql插入语句
            for cidx in cf.index:
                valStr=cf[cidx:cidx+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
                valList=valStr.split(',')
                paramStr=''
                for val in valList:
                    if val=='NULL':
                        paramStr=paramStr+'NULL,'
                    else:       
                        paramStr=paramStr+"'"+val+"'"+','
                paramStr=paramStr[:-1]
                sql='insert into s_cashflow_%s values (%s)'%(stockCode[:6],paramStr)
                MysqlProcessor.execSql(mysqlSession,sql,False)  
        
        sql="select table_name from information_schema.tables where table_name='s_income_%s'"%(stockCode[:6])
        res=mysqlProcessor.querySql(sql)
        #如果数据库中还没有这个表，需要建立表格
        if res.empty:     
            ic.to_sql(name='s_income_'+stockCode[:6],
                      con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
        else:
            #数据库中已经有这个表了
            #对income表中的数据，生成sql插入语句
            for iidx in ic.index:
                valStr=ic[iidx:iidx+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
                valList=valStr.split(',')
                paramStr=''
                for val in valList:
                    if val=='NULL':
                        paramStr=paramStr+'NULL,'
                    else:       
                        paramStr=paramStr+"'"+val+"'"+','
                paramStr=paramStr[:-1]
                sql='insert into s_income_%s values (%s)'%(stockCode[:6],paramStr)
                MysqlProcessor.execSql(mysqlSession,sql,False)
        
    
        log.logger.info('处理完%s的财务报表数据'%(stockCode))
        
        partialUpdate(mysqlSession)
        lastDataUpdate(mysqlSession,stockCode,"FR_StockCode")
        
        time.sleep(0.25)
    
    lastDataUpdate(mysqlSession,stockCode, "FR_Date")


#4、获取股票不复权日K线数据，并存入数据库

#找到之前处理的最后一个股票的代码
sql="select content from u_data_desc where content_name='kdata_update_to'"
res=mysqlProcessor.querySql(sql)
kdata_update_to=res.at[0,'content']

for index,stockCode in stockCodeList.items():
    
    if stockCode<=kdata_update_to:
        continue
    
    #将startday到当前日期前一天该股票数据导入数据库
    #获取该股票数据并写入数据库
    sk=tushare.pro_bar(ts_code=stockCode, start_date=startday, end_date=endday)
    
    if sk.empty:
        #如果没有任何返回值，说明该时间段内该股票没有交易数据
        log.logger.warning('%s在%s到%s时段内无交易数据'%(stockCode,startday,endday))
        continue

    sk.sort_index(inplace=True,ascending=False)
    sk.reset_index(inplace=True,drop=True)
    
    sql="select table_name from information_schema.tables where table_name='s_kdata_%s'"%(stockCode[:6])
    res=mysqlProcessor.querySql(sql)
    #如果数据库中还没有这个表，需要建立表格
    if res.empty:   
        #存入数据库
        sk.to_sql(name='s_kdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
    else:
        #如果已经有该表，需要生成sql，插入表格
        #对stock_k_data表中的数据，生成sql插入语句
        for sidx in sk.index:
            valStr=sk[sidx:sidx+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
            valList=valStr.split(',')
            paramStr=''
            for val in valList:
                if val=='NULL':
                    paramStr=paramStr+'NULL,'
                else:       
                    paramStr=paramStr+"'"+val+"'"+','
            paramStr=paramStr[:-1]
            sql='insert into s_kdata_%s values (%s)'%(stockCode[:6],paramStr)
            MysqlProcessor.execSql(mysqlSession,sql,False)
    
    log.logger.info('处理完股票%s在%s到%s区间内的kdata数据'%(stockCode,startday,endday))
    mysqlSession.commit()
    
    partialUpdate(mysqlSession)
    lastDataUpdate(mysqlSession,stockCode, "KD")  
    
    time.sleep(0.9)

#5、获取复权因子数据，并存入数据库

#找到之前处理的最后一个股票的代码
sql="select content from u_data_desc where content_name='adjdata_update_to'"
res=mysqlProcessor.querySql(sql)
adjdata_update_to=res.at[0,'content']


for index,stockCode in stockCodeList.items():

    if stockCode<=adjdata_update_to:
        continue
    
    #获取该股票的复权因子数据并写入数据库
    ad=sdDataAPI.adj_factor(ts_code=stockCode,start_date=startday,end_date=endday)
    
    if ad.empty:
        continue
    
    ad.sort_index(inplace=True,ascending=False)
    
    sql="select table_name from information_schema.tables where table_name='s_adjdata_%s'"%(stockCode[:6])
    res=mysqlProcessor.querySql(sql)
    #如果数据库中还没有这个表，需要建立表格
    if res.empty:
        #存入数据库
        ad.to_sql(name='s_adjdata_'+stockCode[:6], con=mysqlEngine, chunksize=1000, if_exists='replace', index=None)
    else:
        for aidx in ad.index:
            valStr=ad[aidx:aidx+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
            valList=valStr.split(',')
            paramStr=''
            for val in valList:
                if val=='NULL':
                    paramStr=paramStr+'NULL,'
                else:       
                    paramStr=paramStr+"'"+val+"'"+','
            paramStr=paramStr[:-1]
            sql='insert into s_adjdata_%s values (%s)'%(stockCode[:6],paramStr)
            MysqlProcessor.execSql(mysqlSession,sql,False)
    mysqlSession.commit()
    log.logger.info('处理完股票%s的复权因子'%(stockCode))
    
    partialUpdate(mysqlSession)
    lastDataUpdate(mysqlSession,stockCode,"ADJ")  
    
    time.sleep(0.3)

#完成所有数据的更新
totalUpdate(mysqlSession)

#完成所有数据更新，把数据库表重置，以便下一次处理使用
lastDataUpdate(mysqlSession,"","FR_StockCode")
lastDataUpdate(mysqlSession,"","KD")
lastDataUpdate(mysqlSession,"","ADJ")

mysqlSession.close()
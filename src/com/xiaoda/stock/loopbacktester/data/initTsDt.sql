-- 创建数据库及相关表格，并初始化

-- 创建数据库
create database tsdb character set utf8mb4 collate utf8mb4_bin;

-- 选择数据库
use tsdb;


-- 建立用于记录数据库更新进度、数据范围等信息的表
create table u_data_desc (
content_name varchar(100) primary key,
content varchar(200) not null default '',
comments varchar(300) not null default ''
);


-- 初始化数据更新表：
-- 最近一次成功的、完整的数据更新，在完成所有数据更新任务后才更新该字段
-- 这个是自然日时间
insert into u_data_desc (content_name,content,comments) values ('last_totally_update_time','','the time of last total data update');

-- 数据范围起始日期，在完成所有数据更新任务后更新该字段
-- 这个日期，应当是第一个交易日的日期，不是自然日日期
insert into u_data_desc (content_name,content,comments) values ('data_start_dealday','','the start deal day(include) of all data');

-- 数据范围结束日期，在完成所有数据更新任务后更新该字段
-- 这个日期，应当是最后一个交易日的日期，不是自然日日期
insert into u_data_desc (content_name,content,comments) values ('data_end_dealday','','the end deal day(include) of all data');

-- 最近一次数据更新（不论是否完成完整的数据更新），在每个数据更新后都更新该字段
-- 这个是自然日时间
insert into u_data_desc (content_name,content,comments) values ('last_update_time','','the time of last data update');

-- 财务报表最新更新的股票代码，每个股票代码更新财务报表数据后都更新该字段
insert into u_data_desc (content_name,content,comments) values ('finance_report_stockcode_update_to','','the stock code of last update of finance report');

-- 财务报表最新更新的日期，更新完所有股票的财务报表后更新该字段
-- 只有上次更新时间距离当前超过1个月，才会执行更新，否则不执行股票财务报表数据的更新
insert into u_data_desc (content_name,content,comments) values ('finance_report_date_update_to','','the date of last update of finance report');

-- k线数据最新更新的股票代码，每个股票的k线数据更新后都更新该字段
insert into u_data_desc (content_name,content,comments) values ('kdata_update_to','','the stock code of last update of k data');

--股票每日数据最新更新的股票代码，每个股票的每日数据更新后都更新该字段
insert into u_data_desc (content_name,content,comments) values ('dailybasic_update_to','','the stock code of last update of daily basic data');


-- 复权因子数据最新更新的股票代码，每个股票的复权因子数据更新后都更新该字段
insert into u_data_desc (content_name,content,comments) values ('adjdata_update_to','','the stock code of last update of adjdata');



-- 建立用于记录分行业涨跌波动率的表
create table u_vol_for_industry (
industry varchar(100) primary key,
stock_num int default 0,
max_ret_rate float not null default 0,
max_inc_rate float not null default 0,
start_date varchar(8),
end_date varchar(8)
);




-- 建立交易日历表
CREATE TABLE `u_trade_cal` (
  `exchange` text COLLATE utf8mb4_bin,
  `cal_date` varchar(20) COLLATE utf8mb4_bin primary key,
  `is_open` bigint(20) DEFAULT NULL,
  `pretrade_date` text COLLATE utf8mb4_bin
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;



-- 建立股票清单表
CREATE TABLE `u_stock_list` (
  `ts_code` varchar(20) COLLATE utf8mb4_bin primary key,
  `symbol` text COLLATE utf8mb4_bin,
  `name` text COLLATE utf8mb4_bin,
  `area` text COLLATE utf8mb4_bin,
  `industry` text COLLATE utf8mb4_bin,
  `list_date` text COLLATE utf8mb4_bin
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- 建立财务报表三张表
CREATE TABLE `s_fin_report_income` (
  `ts_code` varchar(20) COLLATE utf8mb4_bin,
  `ann_date` text COLLATE utf8mb4_bin,
  `f_ann_date` text COLLATE utf8mb4_bin,
  `end_date` varchar(20) COLLATE utf8mb4_bin,
  `report_type` text COLLATE utf8mb4_bin,
  `comp_type` text COLLATE utf8mb4_bin,
  `basic_eps` double DEFAULT NULL,
  `diluted_eps` double DEFAULT NULL,
  `total_revenue` double DEFAULT NULL,
  `revenue` double DEFAULT NULL,
  `int_income` text COLLATE utf8mb4_bin,
  `prem_earned` text COLLATE utf8mb4_bin,
  `comm_income` text COLLATE utf8mb4_bin,
  `n_commis_income` text COLLATE utf8mb4_bin,
  `n_oth_income` text COLLATE utf8mb4_bin,
  `n_oth_b_income` text COLLATE utf8mb4_bin,
  `prem_income` text COLLATE utf8mb4_bin,
  `out_prem` text COLLATE utf8mb4_bin,
  `une_prem_reser` text COLLATE utf8mb4_bin,
  `reins_income` text COLLATE utf8mb4_bin,
  `n_sec_tb_income` text COLLATE utf8mb4_bin,
  `n_sec_uw_income` text COLLATE utf8mb4_bin,
  `n_asset_mg_income` text COLLATE utf8mb4_bin,
  `oth_b_income` text COLLATE utf8mb4_bin,
  `fv_value_chg_gain` double DEFAULT NULL,
  `invest_income` double DEFAULT NULL,
  `ass_invest_income` double DEFAULT NULL,
  `forex_gain` text COLLATE utf8mb4_bin,
  `total_cogs` double DEFAULT NULL,
  `oper_cost` double DEFAULT NULL,
  `int_exp` text COLLATE utf8mb4_bin,
  `comm_exp` text COLLATE utf8mb4_bin,
  `biz_tax_surchg` double DEFAULT NULL,
  `sell_exp` double DEFAULT NULL,
  `admin_exp` double DEFAULT NULL,
  `fin_exp` double DEFAULT NULL,
  `assets_impair_loss` double DEFAULT NULL,
  `prem_refund` text COLLATE utf8mb4_bin,
  `compens_payout` text COLLATE utf8mb4_bin,
  `reser_insur_liab` text COLLATE utf8mb4_bin,
  `div_payt` text COLLATE utf8mb4_bin,
  `reins_exp` text COLLATE utf8mb4_bin,
  `oper_exp` text COLLATE utf8mb4_bin,
  `compens_payout_refu` text COLLATE utf8mb4_bin,
  `insur_reser_refu` text COLLATE utf8mb4_bin,
  `reins_cost_refund` text COLLATE utf8mb4_bin,
  `other_bus_cost` text COLLATE utf8mb4_bin,
  `operate_profit` double DEFAULT NULL,
  `non_oper_income` double DEFAULT NULL,
  `non_oper_exp` double DEFAULT NULL,
  `nca_disploss` double DEFAULT NULL,
  `total_profit` double DEFAULT NULL,
  `income_tax` double DEFAULT NULL,
  `n_income` double DEFAULT NULL,
  `n_income_attr_p` double DEFAULT NULL,
  `minority_gain` double DEFAULT NULL,
  `oth_compr_income` double DEFAULT NULL,
  `t_compr_income` double DEFAULT NULL,
  `compr_inc_attr_p` double DEFAULT NULL,
  `compr_inc_attr_m_s` double DEFAULT NULL,
  `ebit` double DEFAULT NULL,
  `ebitda` double DEFAULT NULL,
  `insurance_exp` text COLLATE utf8mb4_bin,
  `undist_profit` text COLLATE utf8mb4_bin,
  `distable_profit` text COLLATE utf8mb4_bin,
  `update_flag` TINYINT(1) NOT NULL,
   PRIMARY KEY (`ts_code`,`end_date`,`update_flag`),
   CONSTRAINT `FK_TC_IC` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `s_fin_report_balancesheet` (
  `ts_code` varchar(20) COLLATE utf8mb4_bin,
  `ann_date` text COLLATE utf8mb4_bin,
  `f_ann_date` text COLLATE utf8mb4_bin,
  `end_date` varchar(20) COLLATE utf8mb4_bin,
  `report_type` text COLLATE utf8mb4_bin,
  `comp_type` text COLLATE utf8mb4_bin,
  `total_share` double DEFAULT NULL,
  `cap_rese` double DEFAULT NULL,
  `undistr_porfit` double DEFAULT NULL,
  `surplus_rese` double DEFAULT NULL,
  `special_rese` text COLLATE utf8mb4_bin,
  `money_cap` double DEFAULT NULL,
  `trad_asset` double DEFAULT NULL,
  `notes_receiv` double DEFAULT NULL,
  `accounts_receiv` double DEFAULT NULL,
  `oth_receiv` double DEFAULT NULL,
  `prepayment` double DEFAULT NULL,
  `div_receiv` text COLLATE utf8mb4_bin,
  `int_receiv` double DEFAULT NULL,
  `inventories` double DEFAULT NULL,
  `amor_exp` double DEFAULT NULL,
  `nca_within_1y` text COLLATE utf8mb4_bin,
  `sett_rsrv` text COLLATE utf8mb4_bin,
  `loanto_oth_bank_fi` double DEFAULT NULL,
  `premium_receiv` text COLLATE utf8mb4_bin,
  `reinsur_receiv` text COLLATE utf8mb4_bin,
  `reinsur_res_receiv` text COLLATE utf8mb4_bin,
  `pur_resale_fa` double DEFAULT NULL,
  `oth_cur_assets` text COLLATE utf8mb4_bin,
  `total_cur_assets` double DEFAULT NULL,
  `fa_avail_for_sale` double DEFAULT NULL,
  `htm_invest` double DEFAULT NULL,
  `lt_eqt_invest` double DEFAULT NULL,
  `invest_real_estate` double DEFAULT NULL,
  `time_deposits` text COLLATE utf8mb4_bin,
  `oth_assets` double DEFAULT NULL,
  `lt_rec` text COLLATE utf8mb4_bin,
  `fix_assets` double DEFAULT NULL,
  `cip` double DEFAULT NULL,
  `const_materials` text COLLATE utf8mb4_bin,
  `fixed_assets_disp` double DEFAULT NULL,
  `produc_bio_assets` text COLLATE utf8mb4_bin,
  `oil_and_gas_assets` text COLLATE utf8mb4_bin,
  `intan_assets` double DEFAULT NULL,
  `r_and_d` text COLLATE utf8mb4_bin,
  `goodwill` double DEFAULT NULL,
  `lt_amor_exp` double DEFAULT NULL,
  `defer_tax_assets` double DEFAULT NULL,
  `decr_in_disbur` double DEFAULT NULL,
  `oth_nca` text COLLATE utf8mb4_bin,
  `total_nca` double DEFAULT NULL,
  `cash_reser_cb` double DEFAULT NULL,
  `depos_in_oth_bfi` double DEFAULT NULL,
  `prec_metals` double DEFAULT NULL,
  `deriv_assets` double DEFAULT NULL,
  `rr_reins_une_prem` text COLLATE utf8mb4_bin,
  `rr_reins_outstd_cla` text COLLATE utf8mb4_bin,
  `rr_reins_lins_liab` text COLLATE utf8mb4_bin,
  `rr_reins_lthins_liab` text COLLATE utf8mb4_bin,
  `refund_depos` text COLLATE utf8mb4_bin,
  `ph_pledge_loans` text COLLATE utf8mb4_bin,
  `refund_cap_depos` text COLLATE utf8mb4_bin,
  `indep_acct_assets` text COLLATE utf8mb4_bin,
  `client_depos` text COLLATE utf8mb4_bin,
  `client_prov` text COLLATE utf8mb4_bin,
  `transac_seat_fee` text COLLATE utf8mb4_bin,
  `invest_as_receiv` double DEFAULT NULL,
  `total_assets` double DEFAULT NULL,
  `lt_borr` double DEFAULT NULL,
  `st_borr` double DEFAULT NULL,
  `cb_borr` double DEFAULT NULL,
  `depos_ib_deposits` text COLLATE utf8mb4_bin,
  `loan_oth_bank` double DEFAULT NULL,
  `trading_fl` double DEFAULT NULL,
  `notes_payable` text COLLATE utf8mb4_bin,
  `acct_payable` double DEFAULT NULL,
  `adv_receipts` double DEFAULT NULL,
  `sold_for_repur_fa` double DEFAULT NULL,
  `comm_payable` text COLLATE utf8mb4_bin,
  `payroll_payable` double DEFAULT NULL,
  `taxes_payable` double DEFAULT NULL,
  `int_payable` double DEFAULT NULL,
  `div_payable` double DEFAULT NULL,
  `oth_payable` double DEFAULT NULL,
  `acc_exp` double DEFAULT NULL,
  `deferred_inc` double DEFAULT NULL,
  `st_bonds_payable` text COLLATE utf8mb4_bin,
  `payable_to_reinsurer` text COLLATE utf8mb4_bin,
  `rsrv_insur_cont` text COLLATE utf8mb4_bin,
  `acting_trading_sec` text COLLATE utf8mb4_bin,
  `acting_uw_sec` text COLLATE utf8mb4_bin,
  `non_cur_liab_due_1y` double DEFAULT NULL,
  `oth_cur_liab` double DEFAULT NULL,
  `total_cur_liab` double DEFAULT NULL,
  `bond_payable` double DEFAULT NULL,
  `lt_payable` double DEFAULT NULL,
  `specific_payables` text COLLATE utf8mb4_bin,
  `estimated_liab` double DEFAULT NULL,
  `defer_tax_liab` double DEFAULT NULL,
  `defer_inc_non_cur_liab` text COLLATE utf8mb4_bin,
  `oth_ncl` text COLLATE utf8mb4_bin,
  `total_ncl` double DEFAULT NULL,
  `depos_oth_bfi` double DEFAULT NULL,
  `deriv_liab` double DEFAULT NULL,
  `depos` double DEFAULT NULL,
  `agency_bus_liab` double DEFAULT NULL,
  `oth_liab` double DEFAULT NULL,
  `prem_receiv_adva` text COLLATE utf8mb4_bin,
  `depos_received` text COLLATE utf8mb4_bin,
  `ph_invest` text COLLATE utf8mb4_bin,
  `reser_une_prem` text COLLATE utf8mb4_bin,
  `reser_outstd_claims` text COLLATE utf8mb4_bin,
  `reser_lins_liab` text COLLATE utf8mb4_bin,
  `reser_lthins_liab` text COLLATE utf8mb4_bin,
  `indept_acc_liab` text COLLATE utf8mb4_bin,
  `pledge_borr` text COLLATE utf8mb4_bin,
  `indem_payable` text COLLATE utf8mb4_bin,
  `policy_div_payable` text COLLATE utf8mb4_bin,
  `total_liab` double DEFAULT NULL,
  `treasury_share` text COLLATE utf8mb4_bin,
  `ordin_risk_reser` double DEFAULT NULL,
  `forex_differ` double DEFAULT NULL,
  `invest_loss_unconf` text COLLATE utf8mb4_bin,
  `minority_int` double DEFAULT NULL,
  `total_hldr_eqy_exc_min_int` double DEFAULT NULL,
  `total_hldr_eqy_inc_min_int` double DEFAULT NULL,
  `total_liab_hldr_eqy` double DEFAULT NULL,
  `lt_payroll_payable` text COLLATE utf8mb4_bin,
  `oth_comp_income` double DEFAULT NULL,
  `oth_eqt_tools` double DEFAULT NULL,
  `oth_eqt_tools_p_shr` double DEFAULT NULL,
  `lending_funds` text COLLATE utf8mb4_bin,
  `acc_receivable` text COLLATE utf8mb4_bin,
  `st_fin_payable` text COLLATE utf8mb4_bin,
  `payables` text COLLATE utf8mb4_bin,
  `hfs_assets` text COLLATE utf8mb4_bin,
  `hfs_sales` text COLLATE utf8mb4_bin,
  `update_flag` TINYINT(1) NOT NULL,
   PRIMARY KEY (`ts_code`,`end_date`,`update_flag`),
   CONSTRAINT `FK_TC_BS` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `s_fin_report_cashflow` (
  `ts_code` varchar(20) COLLATE utf8mb4_bin,
  `ann_date` text COLLATE utf8mb4_bin,
  `f_ann_date` text COLLATE utf8mb4_bin,
  `end_date` varchar(20) COLLATE utf8mb4_bin,
  `comp_type` text COLLATE utf8mb4_bin,
  `report_type` text COLLATE utf8mb4_bin,
  `net_profit` double DEFAULT NULL,
  `finan_exp` double DEFAULT NULL,
  `c_fr_sale_sg` double DEFAULT NULL,
  `recp_tax_rends` double DEFAULT NULL,
  `n_depos_incr_fi` text COLLATE utf8mb4_bin,
  `n_incr_loans_cb` text COLLATE utf8mb4_bin,
  `n_inc_borr_oth_fi` text COLLATE utf8mb4_bin,
  `prem_fr_orig_contr` text COLLATE utf8mb4_bin,
  `n_incr_insured_dep` text COLLATE utf8mb4_bin,
  `n_reinsur_prem` text COLLATE utf8mb4_bin,
  `n_incr_disp_tfa` text COLLATE utf8mb4_bin,
  `ifc_cash_incr` text COLLATE utf8mb4_bin,
  `n_incr_disp_faas` text COLLATE utf8mb4_bin,
  `n_incr_loans_oth_bank` text COLLATE utf8mb4_bin,
  `n_cap_incr_repur` text COLLATE utf8mb4_bin,
  `c_fr_oth_operate_a` double DEFAULT NULL,
  `c_inf_fr_operate_a` double DEFAULT NULL,
  `c_paid_goods_s` double DEFAULT NULL,
  `c_paid_to_for_empl` double DEFAULT NULL,
  `c_paid_for_taxes` double DEFAULT NULL,
  `n_incr_clt_loan_adv` text COLLATE utf8mb4_bin,
  `n_incr_dep_cbob` text COLLATE utf8mb4_bin,
  `c_pay_claims_orig_inco` text COLLATE utf8mb4_bin,
  `pay_handling_chrg` text COLLATE utf8mb4_bin,
  `pay_comm_insur_plcy` text COLLATE utf8mb4_bin,
  `oth_cash_pay_oper_act` double DEFAULT NULL,
  `st_cash_out_act` double DEFAULT NULL,
  `n_cashflow_act` double DEFAULT NULL,
  `oth_recp_ral_inv_act` double DEFAULT NULL,
  `c_disp_withdrwl_invest` double DEFAULT NULL,
  `c_recp_return_invest` double DEFAULT NULL,
  `n_recp_disp_fiolta` double DEFAULT NULL,
  `n_recp_disp_sobu` double DEFAULT NULL,
  `stot_inflows_inv_act` double DEFAULT NULL,
  `c_pay_acq_const_fiolta` double DEFAULT NULL,
  `c_paid_invest` double DEFAULT NULL,
  `n_disp_subs_oth_biz` double DEFAULT NULL,
  `oth_pay_ral_inv_act` double DEFAULT NULL,
  `n_incr_pledge_loan` double DEFAULT NULL,
  `stot_out_inv_act` double DEFAULT NULL,
  `n_cashflow_inv_act` double DEFAULT NULL,
  `c_recp_borrow` double DEFAULT NULL,
  `proc_issue_bonds` double DEFAULT NULL,
  `oth_cash_recp_ral_fnc_act` double DEFAULT NULL,
  `stot_cash_in_fnc_act` double DEFAULT NULL,
  `free_cashflow` double DEFAULT NULL,
  `c_prepay_amt_borr` double DEFAULT NULL,
  `c_pay_dist_dpcp_int_exp` double DEFAULT NULL,
  `incl_dvd_profit_paid_sc_ms` text COLLATE utf8mb4_bin,
  `oth_cashpay_ral_fnc_act` double DEFAULT NULL,
  `stot_cashout_fnc_act` double DEFAULT NULL,
  `n_cash_flows_fnc_act` double DEFAULT NULL,
  `eff_fx_flu_cash` double DEFAULT NULL,
  `n_incr_cash_cash_equ` double DEFAULT NULL,
  `c_cash_equ_beg_period` double DEFAULT NULL,
  `c_cash_equ_end_period` double DEFAULT NULL,
  `c_recp_cap_contrib` double DEFAULT NULL,
  `incl_cash_rec_saims` double DEFAULT NULL,
  `uncon_invest_loss` text COLLATE utf8mb4_bin,
  `prov_depr_assets` double DEFAULT NULL,
  `depr_fa_coga_dpba` double DEFAULT NULL,
  `amort_intang_assets` double DEFAULT NULL,
  `lt_amort_deferred_exp` double DEFAULT NULL,
  `decr_deferred_exp` double DEFAULT NULL,
  `incr_acc_exp` double DEFAULT NULL,
  `loss_disp_fiolta` double DEFAULT NULL,
  `loss_scr_fa` double DEFAULT NULL,
  `loss_fv_chg` double DEFAULT NULL,
  `invest_loss` double DEFAULT NULL,
  `decr_def_inc_tax_assets` double DEFAULT NULL,
  `incr_def_inc_tax_liab` double DEFAULT NULL,
  `decr_inventories` double DEFAULT NULL,
  `decr_oper_payable` double DEFAULT NULL,
  `incr_oper_payable` double DEFAULT NULL,
  `others` double DEFAULT NULL,
  `im_net_cashflow_oper_act` double DEFAULT NULL,
  `conv_debt_into_cap` text COLLATE utf8mb4_bin,
  `conv_copbonds_due_within_1y` text COLLATE utf8mb4_bin,
  `fa_fnc_leases` double DEFAULT NULL,
  `end_bal_cash` double DEFAULT NULL,
  `beg_bal_cash` double DEFAULT NULL,
  `end_bal_cash_equ` text COLLATE utf8mb4_bin,
  `beg_bal_cash_equ` text COLLATE utf8mb4_bin,
  `im_n_incr_cash_equ` double DEFAULT NULL,
  `update_flag` TINYINT(1) NOT NULL,
   PRIMARY KEY (`ts_code`,`end_date`,`update_flag`),
   CONSTRAINT `FK_TC_CF` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- 建立K线数据表(不复权)
CREATE TABLE `s_k_data` (
  `ts_code` varchar(20) COLLATE utf8mb4_bin,
  `trade_date` varchar(20) COLLATE utf8mb4_bin,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `pre_close` double DEFAULT NULL,
  `change` double DEFAULT NULL,
  `pct_chg` double DEFAULT NULL,
  `vol` double DEFAULT NULL,
  `amount` double DEFAULT NULL,
   PRIMARY KEY (`ts_code`,`trade_date`),
   CONSTRAINT `FK_TC_KD` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;



-- 建立每日指标表
CREATE TABLE `s_daily_basic` (
  `ts_code` varchar(20) COLLATE utf8mb4_bin,
  `trade_date` varchar(20) COLLATE utf8mb4_bin,
  `close` double DEFAULT NULL,
  `turnover_rate` double DEFAULT NULL,
  `turnover_rate_f` double DEFAULT NULL,
  `volume_ratio` double DEFAULT NULL,
  `pe` double DEFAULT NULL,
  `pe_ttm` double DEFAULT NULL,
  `pb` double DEFAULT NULL,
  `ps` double DEFAULT NULL,
  `ps_ttm` double DEFAULT NULL,
  `dv_ratio` double DEFAULT NULL,
  `dv_ttm` double DEFAULT NULL,
  `total_share` double DEFAULT NULL,
  `float_share` double DEFAULT NULL,
  `free_share` double DEFAULT NULL,
  `total_mv` double DEFAULT NULL,
  `circ_mv` double DEFAULT NULL,
   PRIMARY KEY (`ts_code`,`trade_date`),
   CONSTRAINT `FK_TC_DB` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- 建立复权因子表
CREATE TABLE `s_adj_data` (
  `ts_code` varchar(20) COLLATE utf8mb4_bin,
  `trade_date` varchar(20) COLLATE utf8mb4_bin,
  `adj_factor` double DEFAULT NULL,
   PRIMARY KEY (`ts_code`,`trade_date`),
   CONSTRAINT `FK_TC_ADJ` FOREIGN KEY (`ts_code`) REFERENCES `u_stock_list` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
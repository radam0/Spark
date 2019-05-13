DROP TABLE IF EXISTS stage_load.t_stg_efe_all_ssn ;
CREATE EXTERNAL TABLE stage_load.t_stg_efe_all_ssn (
ssn_n string,
eldry_addr_upd_d string,
eldry_age string,
eldry_i string,
eldry_all_acc_rstc_c string,
eldry_birth_d string,
eldry_citz_cntry_c string,
eldry_cm_addr1 string,
eldry_cm_addr2 string,
eldry_cm_city_nm string,
eldry_cm_st_c string,
eldry_cm_zip_c string,
eldry_death_d string,
eldry_eml_upd_by string,
eldry_eml_upd_d string,
eldry_fst_nm string,
eldry_lst_nm string,
eldry_mail_sup_i string,
eldry_mail_sup_pnd_c string,
eldry_mid_nm string,
eldry_nm_sufx string,
eldry_prm_eml_addr string,
eldry_net_wrth_a string,
eldry_seg_c string,
insert_tmst string,
run_d string,
run_usr_id string )
row format delimited
fields terminated by ','
location "/user/hive/warehouse/stage_load.db/t_stg_efe_all_ssn";

DROP TABLE IF EXISTS stage_load.t_efe_cust_seg_hist ;
CREATE EXTERNAL TABLE stage_load.t_efe_cust_seg_hist (
ssn_hist_id string,
ssn_n string,
seg_c string,
eff_d string,
exp_d string,
insert_tmst string,
run_d string,
run_usr_id string
)
row format delimited
fields terminated by ','
location "/user/hive/warehouse/stage_load.db/t_efe_cust_seg_hist";

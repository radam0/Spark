from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession \
    .builder \
    .appName("SCD Type 1") \
    .getOrCreate()


#read the tables into df
df_main_tbl=spark.sql("""select * from stage_load.t_care_ca_rel_typ1_main""")
df_prev_tbl=spark.sql("""select * from stage_load.t_care_ca_rel_typ1_prev""")
df_src_tbl=spark.sql("""select * from stage_load.t_care_ca_rel_typ1_src""")


#registered temporary for use
df_src_tbl.createOrReplaceTempView("temp_tbl_src")
df_prev_tbl.createOrReplaceTempView("temp_tbl_prev")

#last_update_dt="2019-03-01"

#lets do a full outer join to get all the new updated and deleted records and assign it to a dataframe df_records
df_records=spark.sql("""
select
src.prtn_id as src_prtn_id
,src.cus_id as src_cus_id
,src.fbsi_firm_c as src_fbsi_firm_c
,src.fbsi_brch_c as src_fbsi_brch_c
,src.fbsi_base_c as src_fbsi_base_c
,src.fid_acc_id as src_fid_acc_id
,src.rel_ty_c as src_rel_ty_c
,src.seq_n as src_seq_n 
,src.prnc_nm_c as src_prnc_nm_c
,src.multi_co_n as src_multi_co_n
,src.cre_tmst as src_cre_tmst
,src.cre_user as src_cre_user
,src.upd_tmst as src_upd_tmst
,src.upd_usr as src_upd_usr 
,src.email_ad_of_rec_c as src_email_ad_of_rec_c
,src.prm_cus_c as src_prm_cus_c
,src.ibmsnap_intentseq as src_ibmsnap_intentseq
,src.ibmsnap_commitseq as src_ibmsnap_commitseq
,src.ibmsnap_operation as src_ibmsnap_operation
,src.ibmsnap_logmarker as src_ibmsnap_logmarker
,src.rel_to_tre_c as src_rel_to_tre_c
,src.tst_cont_opt_out_c as src_tst_cont_opt_out_c
,src.run_d as src_run_d
,src.hdfs_insert_tmst as src_hdfs_insert_tmst
,src.hdfs_update_tmst as src_hdfs_update_tmst
,src.hdfs_update_user as src_hdfs_update_user
----------------------------
,prev.prtn_id as prev_prtn_id
,prev.cus_id as prev_cus_id
,prev.fbsi_firm_c as prev_fbsi_firm_c
,prev.fbsi_brch_c as prev_fbsi_brch_c
,prev.fbsi_base_c as prev_fbsi_base_c
,prev.fid_acc_id as prev_fid_acc_id
,prev.rel_ty_c as prev_rel_ty_c
,prev.seq_n  as prev_seq_n 
,prev.prnc_nm_c as prev_prnc_nm_c
,prev.multi_co_n as prev_multi_co_n
,prev.cre_tmst as prev_cre_tmst
,prev.cre_user as prev_cre_user
,prev.upd_tmst as prev_upd_tmst
,prev.upd_usr  as prev_upd_usr 
,prev.email_ad_of_rec_c as prev_email_ad_of_rec_c
,prev.prm_cus_c as prev_prm_cus_c
,prev.ibmsnap_intentseq as prev_ibmsnap_intentseq
,prev.ibmsnap_commitseq as prev_ibmsnap_commitseq
,prev.ibmsnap_operation as prev_ibmsnap_operation
,prev.ibmsnap_logmarker as prev_ibmsnap_logmarker
,prev.rel_to_tre_c as prev_rel_to_tre_c
,prev.tst_cont_opt_out_c as prev_tst_cont_opt_out_c
,prev.run_d as prev_run_d
,prev.hdfs_insert_tmst as prev_hdfs_insert_tmst
,prev.hdfs_update_tmst as prev_hdfs_update_tmst
,prev.hdfs_update_user as prev_hdfs_update_user
from temp_tbl_prev prev
FULL join
temp_tbl_src src
on
src.prtn_id = prev.prtn_id
and src.cus_id = prev.cus_id
and src.fbsi_firm_c = prev.fbsi_firm_c
and src.fbsi_brch_c = prev.fbsi_brch_c
and src.fbsi_base_c = prev.fbsi_base_c
and src.fid_acc_id  = prev.fid_acc_id
and src.rel_ty_c = prev.rel_ty_c
and src.multi_co_n = prev.multi_co_n
""")

# create temp table ll the new updated and deleted records.
df_records.createOrReplaceTempView("temp_df_records")


# now lets find all the new records.
df_new_records=spark.sql("""
SELECT
src_prtn_id as prtn_id
,src_cus_id as cus_id
,src_fbsi_firm_c as fbsi_firm_c
,src_fbsi_brch_c as fbsi_brch_c
,src_fbsi_base_c as fbsi_base_c
,src_fid_acc_id as fid_acc_id
,src_rel_ty_c as rel_ty_c
,src_seq_n as seq_n
,src_prnc_nm_c as prnc_nm_c
,src_multi_co_n as multi_co_n
,src_cre_tmst as cre_tmst
,src_cre_user as cre_user
,src_upd_tmst as upd_tmst
,src_upd_usr as upd_usr
,src_email_ad_of_rec_c as email_ad_of_rec_c
,src_prm_cus_c as prm_cus_c
,src_ibmsnap_intentseq as ibmsnap_intentseq
,src_ibmsnap_commitseq as ibmsnap_commitseq
,src_ibmsnap_operation as ibmsnap_operation
,src_ibmsnap_logmarker as ibmsnap_logmarker
,src_rel_to_tre_c as rel_to_tre_c
,src_tst_cont_opt_out_c as tst_cont_opt_out_c
,src_run_d as run_d
,src_hdfs_insert_tmst as hdfs_insert_tmst
,src_hdfs_update_tmst as hdfs_update_tmst
,src_hdfs_update_user as hdfs_update_user
FROM
temp_df_records
WHERE
prev_prtn_id IS NULL
AND prev_cus_id IS NULL
AND prev_fbsi_firm_c IS NULL
AND prev_fbsi_brch_c IS NULL
AND prev_fbsi_base_c IS NULL
AND prev_fid_acc_id IS NULL
AND prev_rel_ty_c IS NULL
AND prev_multi_co_n  IS NULL
AND src_prtn_id IS NOT NULL
AND src_cus_id IS NOT NULL
AND src_fbsi_firm_c IS NOT NULL
AND src_fbsi_brch_c IS NOT NULL
AND src_fbsi_base_c IS NOT NULL
AND src_fid_acc_id IS NOT NULL
AND src_rel_ty_c IS NOT NULL
AND src_multi_co_n IS NOT NULL
""")

# now lets find all the upateded records from source.
df_update_records=spark.sql("""
SELECT
src_prtn_id as prtn_id
,src_cus_id as cus_id
,src_fbsi_firm_c as fbsi_firm_c
,src_fbsi_brch_c as fbsi_brch_c
,src_fbsi_base_c as fbsi_base_c
,src_fid_acc_id as fid_acc_id
,src_rel_ty_c as rel_ty_c
,src_seq_n as seq_n
,src_prnc_nm_c as prnc_nm_c
,src_multi_co_n as multi_co_n
,src_cre_tmst as cre_tmst
,src_cre_user as cre_user
,src_upd_tmst as upd_tmst
,src_upd_usr as upd_usr
,src_email_ad_of_rec_c as email_ad_of_rec_c
,src_prm_cus_c as prm_cus_c
,src_ibmsnap_intentseq as ibmsnap_intentseq
,src_ibmsnap_commitseq as ibmsnap_commitseq
,src_ibmsnap_operation as ibmsnap_operation
,src_ibmsnap_logmarker as ibmsnap_logmarker
,src_rel_to_tre_c as rel_to_tre_c
,src_tst_cont_opt_out_c as tst_cont_opt_out_c
,src_run_d as run_d
,src_hdfs_insert_tmst as hdfs_insert_tmst
,src_hdfs_update_tmst as hdfs_update_tmst
,src_hdfs_update_user as hdfs_update_user
FROM
temp_df_records
WHERE
prev_prtn_id IS NOT NULL
AND prev_cus_id IS NOT NULL
AND prev_fbsi_firm_c IS NOT NULL
AND prev_fbsi_brch_c IS NOT NULL
AND prev_fbsi_base_c IS NOT NULL
AND prev_fid_acc_id IS NOT NULL
AND prev_rel_ty_c IS NOT NULL
AND prev_multi_co_n  IS NOT NULL
AND src_prtn_id IS NOT NULL
AND src_cus_id IS NOT NULL
AND src_fbsi_firm_c IS NOT NULL
AND src_fbsi_brch_c IS NOT NULL
AND src_fbsi_base_c IS NOT NULL
AND src_fid_acc_id IS NOT NULL
AND src_rel_ty_c IS NOT NULL
AND src_multi_co_n IS NOT NULL
----------------------------
AND src_seq_n  <> prev_seq_n 
or src_prnc_nm_c <> prev_prnc_nm_c
or src_cre_tmst <> prev_cre_tmst
or src_cre_user <> prev_cre_user
or src_upd_tmst <> prev_upd_tmst
or src_upd_usr  <> prev_upd_usr 
or src_email_ad_of_rec_c <> prev_email_ad_of_rec_c
or src_prm_cus_c <> prev_prm_cus_c
or src_ibmsnap_intentseq <> prev_ibmsnap_intentseq
or src_ibmsnap_commitseq <> prev_ibmsnap_commitseq
or src_ibmsnap_operation <> prev_ibmsnap_operation
or src_ibmsnap_logmarker <> prev_ibmsnap_logmarker
or src_rel_to_tre_c <> prev_rel_to_tre_c
or src_tst_cont_opt_out_c <> prev_tst_cont_opt_out_c
""")
df_update_records.cache()

#now lets find all the updated records from main table which should be replaced.
df_update_sub_records=spark.sql("""
SELECT
prev_prtn_id as prtn_id
,prev_cus_id as cus_id
,prev_fbsi_firm_c as fbsi_firm_c
,prev_fbsi_brch_c as fbsi_brch_c
,prev_fbsi_base_c as fbsi_base_c
,prev_fid_acc_id as fid_acc_id
,prev_rel_ty_c as rel_ty_c
,prev_seq_n as seq_n
,prev_prnc_nm_c as prnc_nm_c
,prev_multi_co_n as multi_co_n
,prev_cre_tmst as cre_tmst
,prev_cre_user as cre_user
,prev_upd_tmst as upd_tmst
,prev_upd_usr as upd_usr
,prev_email_ad_of_rec_c as email_ad_of_rec_c
,prev_prm_cus_c as prm_cus_c
,prev_ibmsnap_intentseq as ibmsnap_intentseq
,prev_ibmsnap_commitseq as ibmsnap_commitseq
,prev_ibmsnap_operation as ibmsnap_operation
,prev_ibmsnap_logmarker as ibmsnap_logmarker
,prev_rel_to_tre_c as rel_to_tre_c
,prev_tst_cont_opt_out_c as tst_cont_opt_out_c
,prev_run_d as run_d
,prev_hdfs_insert_tmst as hdfs_insert_tmst
,prev_hdfs_update_tmst as hdfs_update_tmst
,prev_hdfs_update_user as hdfs_update_user
FROM
temp_df_records
WHERE
prev_prtn_id IS NOT NULL
AND prev_cus_id IS NOT NULL
AND prev_fbsi_firm_c IS NOT NULL
AND prev_fbsi_brch_c IS NOT NULL
AND prev_fbsi_base_c IS NOT NULL
AND prev_fid_acc_id IS NOT NULL
AND prev_rel_ty_c IS NOT NULL
AND prev_multi_co_n  IS NOT NULL
AND src_prtn_id IS NOT NULL
AND src_cus_id IS NOT NULL
AND src_fbsi_firm_c IS NOT NULL
AND src_fbsi_brch_c IS NOT NULL
AND src_fbsi_base_c IS NOT NULL
AND src_fid_acc_id IS NOT NULL
AND src_rel_ty_c IS NOT NULL
AND src_multi_co_n IS NOT NULL
----------------------------
AND src_seq_n  <> prev_seq_n 
or src_prnc_nm_c <> prev_prnc_nm_c
or src_cre_tmst <> prev_cre_tmst
or src_cre_user <> prev_cre_user
or src_upd_tmst <> prev_upd_tmst
or src_upd_usr  <> prev_upd_usr 
or src_email_ad_of_rec_c <> prev_email_ad_of_rec_c
or src_prm_cus_c <> prev_prm_cus_c
or src_ibmsnap_intentseq <> prev_ibmsnap_intentseq
or src_ibmsnap_commitseq <> prev_ibmsnap_commitseq
or src_ibmsnap_operation <> prev_ibmsnap_operation
or src_ibmsnap_logmarker <> prev_ibmsnap_logmarker
or src_rel_to_tre_c <> prev_rel_to_tre_c
or src_tst_cont_opt_out_c <> prev_tst_cont_opt_out_c
""")
df_update_sub_records.cache()

#now time to find no change records.
df_no_update_records=spark.sql("""
SELECT
 prev_prtn_id as prtn_id
,prev_cus_id as cus_id
,prev_fbsi_firm_c as fbsi_firm_c
,prev_fbsi_brch_c as fbsi_brch_c
,prev_fbsi_base_c as fbsi_base_c
,prev_fid_acc_id as fid_acc_id
,prev_rel_ty_c as rel_ty_c
,prev_seq_n as seq_n
,prev_prnc_nm_c as prnc_nm_c
,prev_multi_co_n as multi_co_n
,prev_cre_tmst as cre_tmst
,prev_cre_user as cre_user
,prev_upd_tmst as upd_tmst
,prev_upd_usr as upd_usr
,prev_email_ad_of_rec_c as email_ad_of_rec_c
,prev_prm_cus_c as prm_cus_c
,prev_ibmsnap_intentseq as ibmsnap_intentseq
,prev_ibmsnap_commitseq as ibmsnap_commitseq
,prev_ibmsnap_operation as ibmsnap_operation
,prev_ibmsnap_logmarker as ibmsnap_logmarker
,prev_rel_to_tre_c as rel_to_tre_c
,prev_tst_cont_opt_out_c as tst_cont_opt_out_c
,prev_run_d as run_d
,prev_hdfs_insert_tmst as hdfs_insert_tmst
,prev_hdfs_update_tmst as hdfs_update_tmst
,prev_hdfs_update_user as hdfs_update_user
FROM
temp_df_records
WHERE
prev_prtn_id IS NOT NULL
AND prev_cus_id IS NOT NULL
AND prev_fbsi_firm_c IS NOT NULL
AND prev_fbsi_brch_c IS NOT NULL
AND prev_fbsi_base_c IS NOT NULL
AND prev_fid_acc_id IS NOT NULL
AND prev_rel_ty_c IS NOT NULL
AND prev_multi_co_n  IS NOT NULL
AND src_prtn_id IS NOT NULL
AND src_cus_id IS NOT NULL
AND src_fbsi_firm_c IS NOT NULL
AND src_fbsi_brch_c IS NOT NULL
AND src_fbsi_base_c IS NOT NULL
AND src_fid_acc_id IS NOT NULL
AND src_rel_ty_c IS NOT NULL
AND src_multi_co_n IS NOT NULL
""")

#df_no_update_records      minus        df_update_sub_records
df_no_update_records=df_no_update_records.subtract(df_update_sub_records)


df_temp_final=df_new_records.unionAll(df_update_records).unionAll(df_no_update_records)
df_temp_final.createOrReplaceTempView("final_table")
	
spark.sql("""INSERT OVERWRITE TABLE stage_load.t_care_ca_rel_typ1_main select * from final_table""")
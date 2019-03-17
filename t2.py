#import library 
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext,HiveContext
from pyspark.sql import functions as F
from pyspark.sql import types as pt

conf = SparkConf().setAppName("create sub set")
sc = SparkContext(conf=conf)
sq = HiveContext(sc)

tgt_columns = ['PRTN_ID','CUS_ID','FBSI_FIRM_C','FBSI_BRCH_C','FBSI_BASE_C','FID_ACC_ID', \
'REL_TY_C','SEQ_N','PRNC_NM_C','MULTI_CO_N','CRE_TMST','CRE_USER','UPD_TMST','UPD_USR', \
'EMAIL_AD_OF_REC_C','PRM_CUS_C','IBMSNAP_INTENTSEQ','IBMSNAP_COMMITSEQ','IBMSNAP_OPERATION', \
'IBMSNAP_LOGMARKER','REL_TO_TRE_C','TST_CONT_OPT_OUT_C','ACTIVE_IND','RUN_D','HDFS_INSERT_TMST' \
,'HDFS_UPDATE_TMST','HDFS_UPDATE_USER']

tgt_rename_col = ['TGT_PRTN_ID','TGT_CUS_ID','TGT_FBSI_FIRM_C','TGT_FBSI_BRCH_C','TGT_FBSI_BASE_C', \
'TGT_FID_ACC_ID','TGT_REL_TY_C','TGT_SEQ_N','TGT_PRNC_NM_C','TGT_MULTI_CO_N','TGT_CRE_TMST', \
'TGT_CRE_USER','TGT_UPD_TMST','TGT_UPD_USR','TGT_EMAIL_AD_OF_REC_C','TGT_PRM_CUS_C', \
'TGT_IBMSNAP_INTENTSEQ','TGT_IBMSNAP_COMMITSEQ','TGT_IBMSNAP_OPERATION','TGT_IBMSNAP_LOGMARKER', \
'TGT_REL_TO_TRE_C','TGT_TST_CONT_OPT_OUT_C','TGT_ACTIVE_IND','TGT_RUN_D','TGT_HDFS_INSERT_TMST', \
'TGT_HDFS_UPDATE_TMST','TGT_HDFS_UPDATE_USER']

stg_columns = ['CUS_ID','FBSI_FIRM_C','FBSI_BRCH_C','FBSI_BASE_C','FID_ACC_ID','REL_TY_C','SEQ_N', \
'PRNC_NM_C','MULTI_CO_N','CRE_TMST','CRE_USER','UPD_TMST','UPD_USR','EMAIL_AD_OF_REC_C','PRM_CUS_C', \
'IBMSNAP_INTENTSEQ','IBMSNAP_COMMITSEQ','IBMSNAP_OPERATION','IBMSNAP_LOGMARKER','REL_TO_TRE_C', \
'TST_CONT_OPT_OUT_C','ACTIVE_IND','RUN_D','HDFS_INSERT_TMST','HDFS_UPDATE_TMST','HDFS_UPDATE_USER']

stg_rename_col = ['STG_CUS_ID','STG_FBSI_FIRM_C','STG_FBSI_BRCH_C','STG_FBSI_BASE_C','STG_FID_ACC_ID' \
,'STG_REL_TY_C','STG_SEQ_N','STG_PRNC_NM_C','STG_MULTI_CO_N','STG_CRE_TMST','STG_CRE_USER','STG_UPD_TMST' \
, 'STG_UPD_USR','STG_EMAIL_AD_OF_REC_C','STG_PRM_CUS_C','STG_IBMSNAP_INTENTSEQ','STG_IBMSNAP_COMMITSEQ' \
 ,'STG_IBMSNAP_OPERATION','STG_IBMSNAP_LOGMARKER','STG_REL_TO_TRE_C','STG_TST_CONT_OPT_OUT_C','STG_ACTIVE_IND' \
 ,'STG_RUN_D','STG_HDFS_INSERT_TMST','STG_HDFS_UPDATE_TMST','STG_HDFS_UPDATE_USER']


# loading the table as dataframe
tgt = sq.table('test_load.T_CARE_CA_REL_CRR')
stg = sq.table('test_load.STG_CARE_CA_REL_CRR')

# renaming the column name for tgt
for each_col in range(len(tgt_columns)):
	tgt = tgt.withColumnRename(tgt_columns[each_col],tgt_rename_col[each_col])

# renaming the dataframe for stg
for each_col in range(len(stg_columns)):
	stg = stg.withColumnRename(stg_columns[each_col],stg_rename_col[each_col])

# joined the dataframe
foj_df = tgt.join(stg,cond,'full_outer')

# --brand new records unmatched on target
fog_df_1 = fog_df.select('STG_CUS_ID','STG_FBSI_FIRM_C','STG_FBSI_BRCH_C','STG_FBSI_BASE_C','STG_FID_ACC_ID','STG_REL_TY_C' \
	,'STG_SEQ_N','STG_PRNC_NM_C','STG_MULTI_CO_N','STG_CRE_TMST','STG_CRE_USER','STG_UPD_TMST','STG_UPD_USR' \
	,'STG_EMAIL_AD_OF_REC_C','STG_PRM_CUS_C','STG_IBMSNAP_INTENTSEQ','STG_IBMSNAP_COMMITSEQ','STG_IBMSNAP_OPERATION' \
	,'STG_IBMSNAP_LOGMARKER','STG_REL_TO_TRE_C','STG_TST_CONT_OPT_OUT_C','STG_RUN_D','STG_HDFS_INSERT_TMST' \
	,'STG_HDFS_UPDATE_TMST','STG_HDFS_UPDATE_USER').where(fog_df.TGT_PRTN_ID.isNull() & fog_df.TGT_CUS_ID.isNull()  \
	& fog_df.TGT_FBSI_FIRM_C.isNull() & fog_df.TGT_FBSI_BASE_C.isNull() & fog_df.TGT_FBSI_BRCH_C.isNull() \
	 & fog_df.TGT_FID_ACC_ID.isNull() & fog_df.TGT_REL_TY_C.isNull() & fog_df.TGT_MULTI_CO_N.isNull()) \
	.withColumn('ACTIVE_IND',F.lit('A').cast(pt.StringType()))

#-- stage records replacing target active records
fog_df_2 = fog_df.select('STG_CUS_ID','STG_FBSI_FIRM_C','STG_FBSI_BRCH_C','STG_FBSI_BASE_C','STG_FID_ACC_ID','STG_REL_TY_C' \
	,'STG_SEQ_N','STG_PRNC_NM_C','STG_MULTI_CO_N','STG_CRE_TMST','STG_CRE_USER','STG_UPD_TMST','STG_UPD_USR' \
	,'STG_EMAIL_AD_OF_REC_C','STG_PRM_CUS_C','STG_IBMSNAP_INTENTSEQ','STG_IBMSNAP_COMMITSEQ','STG_IBMSNAP_OPERATION' \
	,'STG_IBMSNAP_LOGMARKER','STG_REL_TO_TRE_C','STG_TST_CONT_OPT_OUT_C','STG_RUN_D','STG_HDFS_INSERT_TMST' \
	,'STG_HDFS_UPDATE_TMST','STG_HDFS_UPDATE_USER').where((F.col('STG_CUS_ID') == F.col('TGT_CUS_ID')) & \
	(F.col('STG_FBSI_FIRM_C') == F.col('TGT_FBSI_FIRM_C')) & \
	(F.col('STG_FBSI_BRCH_C') == F.col('TGT_FBSI_BRCH_C')) & \
	(F.col('STG_FBSI_BASE_C') == F.col('TGT_FBSI_BASE_C')) & \
	(F.col('STG_FID_ACC_ID') == F.col('TGT_FID_ACC_ID')) & \
	(F.col('STG_REL_TY_C') == F.col('TGT_REL_TY_C')) & \
	(F.col('STG_MULTI_CO_N') == F.col('TGT_MULTI_CO_N')) & \
	(F.col('TGT_ACTIVE_IND') == 'A' )) \
	.withColumn('ACTIVE_IND',F.lit('A').cast(pt.StringType()))
# -- Target records being set inactive
fog_df_3_col = ['TGT_PRTN_ID','TGT_CUS_ID','TGT_FBSI_FIRM_C','TGT_FBSI_BRCH_C','TGT_FBSI_BASE_C','TGT_FID_ACC_ID' \
,'TGT_REL_TY_C','TGT_SEQ_N','TGT_PRNC_NM_C','TGT_MULTI_CO_N','TGT_CRE_TMST','TGT_CRE_USER','TGT_UPD_TMST','TGT_UPD_USR' \
,'TGT_EMAIL_AD_OF_REC_C','TGT_PRM_CUS_C','TGT_IBMSNAP_INTENTSEQ','TGT_IBMSNAP_COMMITSEQ','TGT_IBMSNAP_OPERATION' \
,'TGT_IBMSNAP_LOGMARKER','TGT_REL_TO_TRE_C','TGT_TST_CONT_OPT_OUT_C','TGT_RUN_D','TGT_HDFS_INSERT_TMST' \
,'TGT_HDFS_UPDATE_TMST','TGT_HDFS_UPDATE_USER']

fog_df_3_col_rename = ['STG_PRTN_ID','STG_CUS_ID','STG_FBSI_FIRM_C','STG_FBSI_BRCH_C','STG_FBSI_BASE_C','STG__FID_ACC_ID' \
,'STG_REL_TY_C','STG_SEQ_N','STG_PRNC_NM_C','STG_MULTI_CO_N','STG_CRE_TMST','STG_CRE_USER','STG_UPD_TMST','STG_UPD_USR' \
,'STG_EMAIL_AD_OF_REC_C','STG_PRM_CUS_C','STG_IBMSNAP_INTENTSEQ','STG_IBMSNAP_COMMITSEQ','STG_IBMSNAP_OPERATION' \
,'STG_IBMSNAP_LOGMARKER','STG_REL_TO_TRE_C','STG_TST_CONT_OPT_OUT_C','STG_RUN_D','STG_HDFS_INSERT_TMST','STG_HDFS_UPDATE_TMST' \
,'STG_HDFS_UPDATE_USER']


fog_df_3 = fog_df.select(fog_df_3_col).where((F.col('STG_CUS_ID') == F.col('TGT_CUS_ID')) & \
	(F.col('STG_FBSI_FIRM_C') == F.col('TGT_FBSI_FIRM_C')) & \
	(F.col('STG_FBSI_BRCH_C') == F.col('TGT_FBSI_BRCH_C')) & \
	(F.col('STG_FBSI_BASE_C') == F.col('TGT_FBSI_BASE_C')) & \
	(F.col('STG_FID_ACC_ID') == F.col('TGT_FID_ACC_ID')) & \
	(F.col('STG_REL_TY_C') == F.col('TGT_REL_TY_C')) & \
	(F.col('STG_MULTI_CO_N') == F.col('TGT_MULTI_CO_N')) & \
	(F.col('TGT_ACTIVE_IND') == 'A' ))





for each_col in range(len(fog_df_3_col)):
	fog_df_3 = fog_df_3.withColumnRename(fog_df_3_col[each_col] , fog_df_3_col_rename[each_col])

fog_df_3 = fog_df_3.withColumn('ACTIVE_IND',F.lit('I').cast(pt.StringType()))

# --unchanged target records
fog_df_4 = fog_df.select(fog_df_3_col.appnd('TGT_ACTIVE_IND')).where(fog_df.STG_CUS_ID.isNull() & fog_df.STG_FBSI_BASE_C.isNull()  \
	& fog_df.STG_FBSI_BRCH_C.isNull() & fog_df.STG_FBSI_FIRM_C.isNull() \
	 & fog_df.STG_FID_ACC_ID.isNull() & fog_df.STG_REL_TY_C.isNull() & fog_df.STG_MULTI_CO_N.isNull())

for each_col in range(len(fog_df_3_col)):
	fog_df_4 = fog_df_4.withColumnRename(fog_df_3_col[each_col] , fog_df_3_col_rename[each_col])

fog_df_4 = fog_df_4.withColumnRename('TGT_ACTIVE_IND','ACTIVE_IND')


# --historical inactive records if any
fog_df_5 = fog_df.select(fog_df_3_col.appnd('TGT_ACTIVE_IND')).where((F.col('STG_CUS_ID') == F.col('TGT_CUS_ID')) & \
	(F.col('STG_FBSI_FIRM_C') == F.col('TGT_FBSI_FIRM_C')) & \
	(F.col('STG_FBSI_BRCH_C') == F.col('TGT_FBSI_BRCH_C')) & \
	(F.col('STG_FBSI_BASE_C') == F.col('TGT_FBSI_BASE_C')) & \
	(F.col('STG_FID_ACC_ID') == F.col('TGT_FID_ACC_ID')) & \
	(F.col('STG_REL_TY_C') == F.col('TGT_REL_TY_C')) & \
	(F.col('STG_MULTI_CO_N') == F.col('TGT_MULTI_CO_N')) & \
	(F.col('TGT_ACTIVE_IND') == 'I' ))

for each_col in range(len(fog_df_3_col)):
	fog_df_5 = fog_df_5.withColumnRename(fog_df_3_col[each_col] , fog_df_3_col_rename[each_col])

fog_df_5 = fog_df_5.withColumnRename('TGT_ACTIVE_IND','ACTIVE_IND')

# Manging the column in same order

fog_df_2 = fog_df_2.select(fog_df_1.columns)
fog_df_3 = fog_df_3.select(fog_df_1.columns)
fog_df_4 = fog_df_4.select(fog_df_1.columns)
fog_df_5 = fog_df_5.select(fog_df_1.columns)

# union the all df
final_df = fog_df_1.unionAll(fog_df_2).unionAll(fog_df_3).unionAll(fog_df_4).unionAll(fog_df_5)


final_df.write.parquet('path')  # or you can save a hive table as well  
#df.write().mode(SaveMode.Overwrite).saveAsTable("dbName.tableName");

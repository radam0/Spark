from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("SQL_PRCD") \
    .getOrCreate()
	
spark.sql("set spark.sql.caseSensitive=false")

#variables
sysdate="2019-03-04"
run_usr_id="efecust"


#read the tables into df
df_efe_cust_seg_hist_tbl=spark.sql("""select * from stage_load.t_efe_cust_seg_hist""")
df_stg_efe_all_ssn_tbl=spark.sql("""select * from stage_load.t_stg_efe_all_ssn""")

#registered temporary for use
df_efe_cust_seg_hist_tbl.createOrReplaceTempView("temp_df_efe_cust_seg_hist_tbl")
df_stg_efe_all_ssn_tbl.createOrReplaceTempView("temp_df_stg_efe_all_ssn_tbl")


df_full_join_records=spark.sql("""
SELECT  
SRC.STG_SSN,
SRC.HIST_SSN_N,
SRC.SEG_C,
SRC.ELDRY_SEG_C,
CASE WHEN SRC.HIST_SSN_N IS NULL THEN 'I' WHEN SRC.STG_SSN IS NULL THEN	 'U' WHEN (SRC.STG_SSN = SRC.STG_SSN AND SRC.SEG_C != SRC.ELDRY_SEG_C) THEN 'UI' END OP_IND
FROM (
	SELECT 
	STG.SSN_N STG_SSN,
	HIST.SSN_N HIST_SSN_N,
	HIST.SEG_C SEG_C,
	STG.ELDRY_SEG_C ELDRY_SEG_C
	FROM 
	temp_df_stg_efe_all_ssn_tbl STG 
	FULL OUTER JOIN
	temp_df_efe_cust_seg_hist_tbl HIST
	ON STG.SSN_N = HIST.SSN_N WHERE HIST.EXP_D is NULL
	) SRC
	WHERE 
	(SRC.HIST_SSN_N IS NULL)
	OR (SRC.STG_SSN IS NULL)
	OR (SRC.STG_SSN = SRC.STG_SSN AND SRC.SEG_C != SRC.ELDRY_SEG_C)
""")


df_full_join_records.createOrReplaceTempView("temp_df_full_join_records_tbl")
df_insert_records = df_full_join_records.filter(df_full_join_records.OP_IND == 'I')


#delete operation -start
df_deletes_records = df_full_join_records.filter(df_full_join_records.OP_IND == 'U')
df_deletes_records.createOrReplaceTempView("temp_df_deletes_records_tbl")
df_deletes_records=spark.sql("""SELECT B.ssn_hist_id,B.ssn_n,B.seg_c,B.eff_d,B.insert_tmst,B.run_d,B.run_usr_id FROM temp_df_deletes_records_tbl A JOIN temp_df_efe_cust_seg_hist_tbl B ON A.HIST_SSN_N = B.ssn_n """)
df_deletes_records=df_deletes_records.withColumn("exp_d", lit(sysdate))
df_deletes_records=df_deletes_records.select(["ssn_hist_id","ssn_n","seg_c","eff_d","exp_d","insert_tmst","run_d","run_usr_id"])
#delete operation -end

#update - Delete operation -start
df_update_records = df_full_join_records.filter(df_full_join_records.OP_IND == 'UI')
df_update_records.createOrReplaceTempView("temp_df_update_records_tbl")
df_update_del_records=spark.sql("""SELECT B.ssn_hist_id,B.ssn_n,B.seg_c,B.eff_d,B.insert_tmst,B.run_d,B.run_usr_id FROM temp_df_update_records_tbl A JOIN temp_df_efe_cust_seg_hist_tbl B ON A.HIST_SSN_N = B.ssn_n """)
df_update_del_records=df_update_del_records.withColumn("exp_d", lit(sysdate))
df_update_del_records=df_update_del_records.select(["ssn_hist_id","ssn_n","seg_c","eff_d","exp_d","insert_tmst","run_d","run_usr_id"])
#df_insert_records=df_insert_records.withColumn("ssn_hist_id", monotonically_increasing_id())


#inster and updates -start
df_ins_insupd=spark.sql("""
select row_number() over (order by stg_ssn,seg_c,eldry_seg_c,op_ind) as ssn_hist_id, 
stg_ssn as ssn_n,eldry_seg_c as seg_c
from 
temp_df_full_join_records_tbl where OP_IND = 'I' or OP_IND = 'UI'
""")
#inster and updates -end

try:
   v_max_ssn_hist_id=int(spark.sql(""" SELECT max(ssn_hist_id) as max_ssn_hist_id FROM temp_df_efe_cust_seg_hist_tbl """).collect()[0][0])
   pass
   
except:
   v_max_ssn_hist_id=0
   pass

df_ins_insupd=df_ins_insupd.withColumn("eff_d", lit(sysdate)).withColumn("exp_d", lit(None)).withColumn("insert_tmst", lit(sysdate)).withColumn("run_d", lit(sysdate)).withColumn("run_usr_id", lit(run_usr_id))
df_ins_insupd=df_ins_insupd.withColumn('ssn_hist_id', df_ins_insupd['ssn_hist_id'] + v_max_ssn_hist_id)

#unchanged_records - start
df_unchngd_records=spark.sql("""SELECT B.ssn_hist_id,B.ssn_n,B.seg_c,B.eff_d,B.exp_d,B.insert_tmst,B.run_d,B.run_usr_id FROM temp_df_efe_cust_seg_hist_tbl B WHERE ssn_n not in (SELECT HIST_SSN_N FROM temp_df_full_join_records_tbl where HIST_SSN_N is not null)""")
#unchanged_records - end

#combine all the dfs
df_temp_final=df_ins_insupd.unionAll(df_update_del_records).unionAll(df_deletes_records).unionAll(df_unchngd_records)
df_temp_final.createOrReplaceTempView("temp_df_temp_final_tbl")

#insert overwrite into the main table
spark.sql("""INSERT OVERWRITE TABLE stage_load.t_efe_cust_seg_hist select * from temp_df_temp_final_tbl""")
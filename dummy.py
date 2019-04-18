#import library
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as pt
import logging



def SP_LOAD_EFE_CUST_SEG_HIST():
    """
    :return
    """
    logger.info("Getting the data table - STG_EFE_ALL_SSN ")

    All_ssn_df = spark.table('ACTIMIZESTG_DBO.STG_EFE_ALL_SSN')
    Seg_hist_df = spark.table('ACTIMIZE_DBO.EFE_CUST_SEG_HIST')
    
    Seg_hist_df = Seg_hist_df.where(Seg_hist_df.EXP_D.isNull())

    src_df = All_ssn_df.join(Seg_hist_df,['SSN_N'],'outer')

    src_df = src_df.select('STG_SSN','HIST_SSN_N','HIST_SSN_N','ELDRY_SEG_C') \
        .withColumn('OP_IND',F.when(src_df.HIST_SSN_N.isNull(),F.lit('I') \
            .cast(pt.StringType())).otherwise(F.when(src_df.STG_SSN.isNull(),F.lit('U').cast(pt.StringType()))) \
                .F.when((F.col('SEG_C') == F.col('ELDRY_SEG_C')),F.lit('UI').cast(pt.StringType())))

    src_df = src_df.where(src_df.HIST_SSN_N.isNull() | src_df.STG_SSN.isNull | \
         (F.col('SEG_C') != F.col('ELDRY_SEG_C')))
    return src_df


def write_csv_to_hdfs(input_df, \
    hdfs_path, \
    file_format='csv', \
    file_saveMode='overwrite', \
    number_of_files=1):
    """
    :param input_df:
    :param hdfs_path:
    :param file_format:
    :param file_saveMode:
    :param number_of_files:
    :return:
    """
    return input_df.coalesce(number_of_files) \
        .write.mode(file_saveMode) \
        .format(file_format) \
        .option("header", "true") \
        .save(hdfs_path)



if __name__ == "__main__":

    logger = logging.getLogger(__name__)
    spark = SparkSession\
    .builder\
    .appName("PythonWordCount")\
    .getOrCreate()
  
    df = SP_LOAD_EFE_CUST_SEG_HIST()

    logger.info("###################saving the put put in csv ########################")

    write_csv_to_hdfs(input_df= df, path = 'add the hdfs path')

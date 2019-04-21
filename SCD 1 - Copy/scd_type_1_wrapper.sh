sh data_prep.sh

if [ $? -eq 0 ]
then
	export SPARK_MAJOR_VERSION=2
	echo "INFO : Calling scd_type_1_script.py"
	echo "INFO : spark-submit scd_type_1_script.py"
	hadoop fs -rm -f -skipTrash hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nikhilvemula.db/t_care_ca_rel_main/*
	spark-submit scd_type_1_script.py
fi
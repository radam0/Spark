#/bin/sh
file="$1"
TabName="$2"
cp $1  sparkBaseinput.conf

sed -i 's/TabName/'$2'/g' sparkBaseinput.conf 

cat sparkBaseinput.conf | spark-shell

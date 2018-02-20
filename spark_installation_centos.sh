#!/bin/bash
# Install Spark on CentOS 7

#Installing Java

wget http://18.216.243.34/extra-package/jdk-8u161-linux-x64.rpm
sudo yum install jdk-8u161-linux-x64.rpm -y
export JAVA_HOME=/usr/java/jdk1.8.0_161
echo "export JAVA_HOME=$JAVA_HOME"

#checks the version of Java	
java -version
	

#Installing Scala

yum install wget -y
wget https://downloads.lightbend.com/scala/2.12.4/scala-2.12.4.tgz
tar xvf scala-2.12.4.tgz
sudo mv scala-2.12.4 /usr/lib
sudo ln -s /usr/lib/scala-2.12.4 /usr/lib/scala
export PATH=$PATH:/usr/lib/scala/bin

#checks the version of scala
scala -version
	

#Installing Spark

wget http://d3kbcqa49mib13.cloudfront.net/spark-1.6.0-bin-hadoop2.6.tgz
tar xvf spark-1.6.0-bin-hadoop2.6.tgz
export SPARK_HOME=$HOME/spark-1.6.0-bin-hadoop2.6
export PATH=$PATH:$SPARK_HOME/bin
	

firewall-cmd --permanent --zone=public --add-port=6066/tcp
firewall-cmd --permanent --zone=public --add-port=7077/tcp
firewall-cmd --permanent --zone=public --add-port=8080-8081/tcp
firewall-cmd --reload
	

echo 'export PATH=$PATH:/usr/lib/scala/bin' >> .bash_profile
echo 'export SPARK_HOME=$HOME/spark-1.6.0-bin-hadoop2.6' >> .bash_profile
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> .bash_profile


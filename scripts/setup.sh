apt-get update -y
wait
apt-get install libsasl2-dev -y
wait
apt-get install libmysqlclient-dev -y
wait
apt-get install python-mysqldb -y
wait
sudo apt-get install unzip -y
wait
sudo apt-get install python-pip -y
wait
pip3 install -r requirements.txt
wait
mkdir jar
cd jar
wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.27.tar.gz
tar -xf mysql-connector-java-8.0.27.tar.gz
mv mysql-connector-java-8.0.27/mysql-connector-java-8.0.27.jar ../../spark/jars/
wait
echo "Finshed Setup"

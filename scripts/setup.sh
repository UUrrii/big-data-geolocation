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
echo "Finshed Setup"

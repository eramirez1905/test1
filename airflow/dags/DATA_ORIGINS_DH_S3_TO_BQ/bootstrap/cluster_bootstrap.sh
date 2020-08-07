#!/bin/bash
#Print all commands and exit if any of them ends with exit code <> 0 
set -x -e
chmod -R +x ./
##JDBC
#sudo aws s3 cp s3://peyabi.code.live/utils/drivers/postgresql-42.2.9.jar /usr/lib/spark/jars/
#sudo aws s3 cp s3://peyabi.code.live/utils/drivers/mysql-connector-java-8.0.19.jar /usr/lib/spark/jars/

##download package
#aws s3 cp s3://peyabi.code.live/bi-core-tech/libraries/python/tableauhyperapi-py.zip tableauhyperapi-py.zip
#aws s3 cp s3://peyabi.code.live/bi-core-tech/libraries/python/tableauserverclient-0.10-py2.py3-none-any.whl tableauserverclient-0.10-py2.py3-none-any.whl
#aws s3 cp s3://peyabi.code.live/bi-core-tech/libraries/python/pantab-1.0.1-cp36-cp36m-manylinux2014_x86_64.whl pantab-1.0.1-cp36-cp36m-manylinux2014_x86_64.whl

#install package
sudo python3.6 -m pip install requests
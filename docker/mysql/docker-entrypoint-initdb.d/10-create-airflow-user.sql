CREATE USER 'airflow'@'%' IDENTIFIED BY 'airflow';
GRANT ALL ON `airflow\_%`.* TO 'airflow'@'%';
FLUSH PRIVILEGES;

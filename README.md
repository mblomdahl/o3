
o3
==

Hadoop-Airflow Analytics.


Development Environment (macOS)
-------------------------------

First setup [conda](https://conda.io/projects/conda/en/latest/) with Python 3.6, then:

    git clone git@github.com:mblomdahl/o3.git
    cd o3/
    wget -P resources/ http://apache.mirrors.spacedump.net/hadoop/common/hadoop-2.9.2/hadoop-2.9.2.tar.gz
    wget -P resources/ http://apache.mirrors.spacedump.net/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
    wget -P resources/ http://apache.mirrors.spacedump.net/hive/hive-2.3.4/apache-hive-2.3.4-bin.tar.gz
    conda create --name o3 --yes python=3.6
    conda activate o3
    conda install --name o3 --file spec-file-osx.txt 
    SLUGIFY_USES_TEXT_UNIDECODE=yes pip install -r requirements.txt
    pip install -e .
    export AIRFLOW_HOME=$(pwd)/airflow_home
    airflow initdb
    # Update newly-generated airflow.cfg in AIRFLOW_HOME by setting `dags_folder=$(pwd)/o3/dags`.
    airflow webserver -p 8080



Links
-----

* https://www.linode.com/docs/databases/hadoop/how-to-install-and-set-up-hadoop-cluster/

* https://www.linode.com/docs/databases/hadoop/install-configure-run-spark-on-top-of-hadoop-yarn-cluster/

* https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties

* https://spark.apache.org/docs/latest/running-on-yarn.html

* https://hadoop.apache.org/docs/r2.9.2/hadoop-project-dist/hadoop-common/ClusterSetup.html

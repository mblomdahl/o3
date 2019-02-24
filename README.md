
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
    wget -P resources/ https://repo.continuum.io/archive/Anaconda3-2018.12-Linux-x86_64.sh
    conda env create --name o3 python=3.6 -f environment-macos.yml
    conda activate o3; pip install -e .
    export AIRFLOW_HOME=$(pwd)/airflow_home
    # Checkout your secret enterprise DAGs into `prod-dags` root dir.
    git clone ssh://git@bitbucket.mycompany.com:7999/~myself/prod-dags.git prod-dags
    airflow initdb
    # Update newly-generated airflow.cfg in AIRFLOW_HOME by setting `dags_folder=$(pwd)/airflow_home/dags`.
    airflow webserver -p 8080


Creating the Conda Environment File
-----------------------------------

These commands needs to be executed on the target platform. The output file can then replace the
corresponding file in this repo, i.e. `environment-linux.yml` and `environment-macos.yml`:

    conda create --name o3 --yes python=3.6
    conda install --name o3 -c conda-forge --yes psycopg2 hdfs3 airflow libhdfs3=2.3.0=1 ansible netaddr \
        ipython pandas fastavro pyhive jupyter xlrd matplotlib paramiko bcrypt requests-futures
    conda activate o3; pip install -e .
    conda env export --name o3 > environment-<platform>.yml


Provisioning
------------

    ansible-playbook -i inventories/<inventory>.ini provision.yml
    
    
Jupyter
-------

During an Ansible provisioning a Jupyter Notebook will be deployed on port 8888 of the provisioned 
server. It can also be executed in a development environment, like so:

    conda activate o3; jupyter notebook --notebook-dir=notebooks   


Links
-----

* https://www.linode.com/docs/databases/hadoop/how-to-install-and-set-up-hadoop-cluster/

* https://www.linode.com/docs/databases/hadoop/install-configure-run-spark-on-top-of-hadoop-yarn-cluster/

* https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties

* https://spark.apache.org/docs/latest/running-on-yarn.html

* https://hadoop.apache.org/docs/r2.9.2/hadoop-project-dist/hadoop-common/ClusterSetup.html

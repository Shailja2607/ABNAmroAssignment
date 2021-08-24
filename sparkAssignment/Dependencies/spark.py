"""
spark.py
~~~~~~~~
Module containing helper function for use with Apache Spark
"""
import os
from os import environ, listdir, path
import json

from pyspark import SparkFiles
from pyspark.sql import SparkSession

import logging
from logging.handlers import RotatingFileHandler


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[]):
    """Start Spark session, get Spark logger.
    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.
    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """

    # detect execution environment
    flag_debug = 'DEBUG' in environ.keys()

    if not flag_debug:
        # get Spark session factory
        spark_builder = (
            SparkSession
                .builder.appName(app_name))
    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
                .builder
                .master(master).appName(app_name))

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    logging.basicConfig(
        handlers=[RotatingFileHandler('logs/my_log.log', maxBytes=100000, backupCount=10)],
        level=logging.INFO,
        format='%(levelname)s:%(message)s'
    )

    # logging.basicConfig(filename='app.log', level=logging.INFO, format='%(levelname)s:%(message)s')
    conf = spark_sess.sparkContext.getConf()
    app_id = conf.get('spark.app.id')
    app_name = conf.get('spark.app.name')
    message_prefix = '<' + app_name + ' ' + app_id + '>'

    return spark_sess, message_prefix, logging


def conf_read():
    try:

        # Reading arguments from config.json
        with open("Properties/config.json", "r") as jsonfile:

            data = json.load(jsonfile)

        logging.info("Reading arguments from config.json ")

        return data

    except Exception as e:

        exception_msg = str(e)

        logging.error("Exception occurred in conf_read - %s" % exception_msg)

        raise

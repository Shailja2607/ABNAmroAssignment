from datetime import datetime

from pyspark.sql import SparkSession, dataframe
import json
from Dependencies.spark import start_spark, conf_read
from datetime import datetime


def main():
    """
        The main function will perform the task:
        1.Reads the two dataset provided
        2.Perform the collation by using the sql query taking arguments from config.json
        the name of the country is taken as argument, we can have more than one countries
        but we need to give the name of the countries in single quotes separated by commas
        3.Saving the output file to the output directory
    """
    try:

        # spark session and logging will be created here
        spark, message_prefix, logging = start_spark(app_name='my_etl_job')

        logging.info("Application Started - at %s  application_name_and_id- %s " % (datetime.now(), message_prefix))

        # reading the property file
        config = conf_read()

        df1, df2 = read_file(spark, logging, config["dataset1"], config["dataset2"])

        # taking name of the country as the filter from config file
        df_output = business_function(spark, logging, df1, df2, config["countryFilter"])

        save_output(logging, df_output, config["output_file"])

        # log the success and terminate Spark application
        logging.info("Application end at  %s application name- %s" % (datetime.now(), message_prefix))

        spark.stop()

        return None

    except Exception as e:

        exception_msg = str(e)

        logging.error("Exception occurred in main pipeline - %s" % exception_msg)

        raise


def read_file(spark, logging, input_file_path1, input_file_path2):
    """
        :param logging: for logging
        :param spark: spark session
        :param input_file_path1: Location of the first dataset provided
        :param input_file_path2: Location of the first dataset
        :return df1,df2 : returns dataframes of the input datasets
        :raise exception with the exception message

    """
    try:

        df1 = spark.read.csv(input_file_path1, header=True, inferSchema=True)

        logging.info("Dataset1 read - %s" % datetime.now())

        df2 = spark.read.csv(input_file_path2, header=True, inferSchema=True)

        logging.info("Dataset2 read - %s" % datetime.now())

        return df1, df2

    except Exception as e:

        exception_msg = str(e)

        logging.error("Exception occurred in conf_read - %s" % exception_msg)

        raise


def business_function(spark, logging, df1: dataframe, df2: dataframe, countryfilter):
    """
       this function will combine the two datasets with the required field, will rename certain functions
       and apply required filters
       :param countryfilter: takes argument as the country from config file
       :param df1: Dataframe containing the first dataset
       :param df2: Dataframe containing the second dataset
       :param logging: for logging
       :param spark: spark session
       :return df_output - output dataframe
       :raise exception with the exception message

    """
    # creating temp table for the two datasets
    try:

        # creating temp table for the two datasets
        df1.createOrReplaceTempView("table1")

        logging.info("Temp table1 created- %s" % datetime.now())

        df2.createOrReplaceTempView("table2")

        logging.info("Temp table2 created- %s" % datetime.now())

        # query to combine the two datasets , renaming certain columns and applying required filter
        query = "select table1.id as client_identifier,table1.email,table1.country, table2.btc_a as bitcoin_address, " \
                "table2.cc_t as " \
                "credit_card_type from table1 inner join table2 on table1.id = table2.id  where table1.country " \
                "in (" + countryfilter + ") "

        df_output = spark.sql(query)

        logging.info("Business logic applied- %s" % datetime.now())

        return df_output

    except Exception as e:

        exception_msg = str(e)

        logging.error("Exception occurred in conf_read - %s" % exception_msg)

        raise


def save_output(logging, df_output: dataframe, output_file):
    """
           this function will save output to the client_data directory
           :param output_file: saves the required csv file
           :param df_output: dataframe containing the output data
           :param logging: for logging
           :return None
           :raise exception with the exception message
    """
    try:

        # saving data to client_data directory
        df_output.write.format('csv').option('header', True).mode('overwrite').save(output_file)

        logging.info("output file is saved %s" % datetime.now())

    except Exception as e:

        exception_msg = str(e)

        logging.error("Exception occurred in conf_read - %s" % exception_msg)

        raise


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

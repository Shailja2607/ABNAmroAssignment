# ABNAmroAssignment
##Purpose:
This repository serves as a solution for the assignment where a company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients 
that they want to collate to starting interfacing more with their clients. 
One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands 
and some of their financial details to starting reaching out to them for a new marketing push.

##Deployment:
I have deployed the project on GitHub to the master branch.
 
##Testing Locally: 
I've developed 2 unit test cases and tried to execute it through travis cli.

##Spark.py:
    This file contain the logic for Start Spark session, get Spark logger.
    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.
    
##etl.py:
    The main function will perform the task:
        1.Reads the two dataset provided
        2.Perform the collation by using the sql query taking arguments from config.json
        the name of the country is taken as argument, we can have more than one countries
        but we need to give the name of the countries in single quotes separated by commas
        3.Saving the output file to the output directory

    
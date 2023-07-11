# Databricks notebook source
# MAGIC %md
# MAGIC ###Custom PII Data Detector #
# MAGIC
# MAGIC <img src="https://i.imgur.com/NNyw4Md.png" width="10%">
# MAGIC
# MAGIC #### Scope
# MAGIC This notebook analyzes a sample of data store in Delta tables to detect PII data
# MAGIC
# MAGIC Here are the steps we execute
# MAGIC * This code works only with DBR 12.2 and above databricks runtime clusters
# MAGIC * Read the first N rows of a delta table specified
# MAGIC * Flag columns which have PII data
# MAGIC * Persist the metadata to delta tables in a database called 'data_profile'
# MAGIC * Summarize the findings
# MAGIC
# MAGIC #### PII data types detected
# MAGIC - SSN
# MAGIC - Credit Card Numbers
# MAGIC - Birth Dates
# MAGIC - Person Name
# MAGIC - Person Gender
# MAGIC - Email
# MAGIC - Address
# MAGIC
# MAGIC

# COMMAND ----------

# Specify the Catalog, Schema and Table Name you want to analyze
catalogName = "spark_catalog"
schemaName = "abe_demo"
tableName = "trips_bronze"

print("This profiler will analyze Table: {} stored in Catalog: {} and database: {}".format(tableName,catalogName,schemaName))

# COMMAND ----------

import json, requests
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

profileDF = spark.sql("select customer_id,customer_name,street,ship_to_address,'1/1/1971' as birth_date,'5555555555554444' as cc_no,units_purchased from abe_demo.customers limit 100")
display(profileDF)

# COMMAND ----------

import re
def checkSSN(ssn):
  ptn=re.compile(r'^\d\d\d-\d\d-\d\d\d\d$')
  return bool(re.match(ptn, ssn))


  

# COMMAND ----------

def format_card(card_num):
    """
    Formats card numbers to remove any spaces, unnecessary characters, etc
    Input: Card number, integer or string
    Output: Correctly formatted card number, string
    """
    import re
    card_num = str(card_num)
    # Regex to remove any nondigit characters
    return re.sub(r"\D", "", card_num)

# COMMAND ----------

def validate_card(formated_card_num):
    """
    Input: Card number, integer or string
    Output: Valid?, boolean
    """
    double = 0
    total = 0

    digits = str(formated_card_num)

    for i in range(len(digits) - 1, -1, -1):
        for c in str((double + 1) * int(digits[i])):
            total += int(c)
        double = (double + 1) % 2

    return (total % 10) == 0

# COMMAND ----------

# Sample Credit Card Numbers

# American Express:378282246310005
# American Express:371449635398431
# Discover:6011111111111117
# Discover:6011000990139424
# MasterCard:5555555555554444
# MasterCard:5105105105105100
# Visa:4111111111111111
# Visa:4012888888881881

# COMMAND ----------

crd = "6011111111111117"
validate_card(crd)

# COMMAND ----------

from dateutil import parser

# input date
date_string = '3-32-2028'

# printing the input date
print("Input Date:", date_string)

# using try-except blocks for handling the exceptions
try:

   # parsing the date string using parse() function
   # It returns true if the date is correctly formatted else it will go to except block
   print(bool(parser.parse(date_string)))

# If the date validation goes wrong
except ValueError:

   # printing the appropriate text if ValueError occurs
   print("Incorrect data format")

# COMMAND ----------

re.compile("^.*(gender).*$"
re.compile("^.*(nationality).*$", re.IGNORECASE),

# COMMAND ----------

date_of_birth|dateofbirth|dob|"
            "birthday|date_of_death|dateofdeat

            address|city|state|county|country|zipcode|postal|zone|borough

# COMMAND ----------

regex = {
        Person: re.compile(
            "^.*(firstname|fname|lastname|lname|"
            "fullname|maidenname|_name|"
            "nickname|name_suffix|name).*$",
            re.IGNORECASE,
        ),
        Email: re.compile("^.*(email|e-mail|mail).*$", re.IGNORECASE),
        BirthDate: re.compile(
            "^.*(date_of_birth|dateofbirth|dob|"
            "birthday|date_of_death|dateofdeath).*$",
            re.IGNORECASE,
        ),
        Gender: re.compile("^.*(gender).*$", re.IGNORECASE),
        Nationality: re.compile("^.*(nationality).*$", re.IGNORECASE),
        Address: re.compile(
            "^.*(address|city|state|county|country|zipcode|postal|zone|borough).*$",
            re.IGNORECASE,
        ),
        UserName: re.compile("^.*user(id|name|).*$", re.IGNORECASE),
        Password: re.compile("^.*pass.*$", re.IGNORECASE),
        SSN: re.compile("^.*(ssn|social).*$", re.IGNORECASE),
    }

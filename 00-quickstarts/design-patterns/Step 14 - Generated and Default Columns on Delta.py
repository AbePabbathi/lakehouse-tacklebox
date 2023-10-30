# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Generate and Default Columns Best Practices and Tutorial
# MAGIC
# MAGIC Purpose: This notebook aims to show how to use generated columns such as IDENTITY keys, DEFAULT values, and other GENERATED columns, and will also walk through limitations as well. 
# MAGIC
# MAGIC ### Covered Topics
# MAGIC
# MAGIC 1. IDENTITY KEYS
# MAGIC 2. DEFAULT columns
# MAGIC 3. GENERATED Expressions
# MAGIC 4. DROP / ALTER COLUMN 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS main.cody_tests;
# MAGIC USE CATALOG main;
# MAGIC USE SCHEMA cody_tests;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS test_ctas SELECT 1;
# MAGIC
# MAGIC -- Key features for data warehousing
# MAGIC ALTER TABLE test_ctas SET TBLPROPERTIES (
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE test_ctas 
# MAGIC (
# MAGIC   Id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   TYPE_ID INT NOT NULL,
# MAGIC   PERSON_ID STRING NOT NULL DEFAULT 'a',
# MAGIC   EVENT_DATE DATE NOT NULL DEFAULT '9999-12-31'::date
# MAGIC )
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC
# MAGIC INSERT INTO test_ctas
# MAGIC (TYPE_ID)
# MAGIC VALUES (1);
# MAGIC
# MAGIC SELECT * FROM test_ctas;

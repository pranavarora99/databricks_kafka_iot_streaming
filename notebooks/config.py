# Databricks notebook source
# Databricks notebook source
class Config():    
    def __init__(self):      
        self.base_dir_data = spark.sql("describe external location `lakeshouse-data`").select("url").collect()[0][0]
        self.base_dir_checkpoint = spark.sql("describe external location `lakehouse-checkpoint`").select("url").collect()[0][0]
        self.db_name = "lakehouse_db"
        self.maxFilesPerTrigger = 1000

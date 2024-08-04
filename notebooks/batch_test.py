# Databricks notebook source
# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

# MAGIC %run ./initial_setup

# COMMAND ----------

SH = SetupHelper(env)
SH.cleanup()


dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

# MAGIC %run ./history_data_loader

# COMMAND ----------

HL = HistoryLoader(env)
SH.validate()
HL.validate()

# COMMAND ----------

# MAGIC %run ./producer

# COMMAND ----------

PR =Producer()
PR.produce(1)
PR.validate(1)
dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

# MAGIC %run ./bronze_layer
# MAGIC %run ./silver_layer
# MAGIC %run ./gold_layer

# COMMAND ----------

BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)
BZ.validate(1)
SL.validate(1)
GL.validate(1)


PR.produce(2)
PR.validate(2)
dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

BZ.validate(2)
SL.validate(2)
GL.validate(2)
SH.cleanup()

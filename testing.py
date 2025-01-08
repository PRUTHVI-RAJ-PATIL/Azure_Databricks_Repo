# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col

# ignore , here im generating the more sample data 

import random
from pyspark.sql.functions import col

# Initialize Spark Session


# Existing Sample Data
simpleData = [
    ("James", "Sales", "NY", 90000, 34, 10000),
    ("Michael", "Sales", "NY", 86000, 56, 20000),
    ("Robert", "Sales", "CA", 81000, 30, 23000),
    ("Maria", "Finance", "CA", 90000, 24, 23000),
    ("Raman", "Finance", "CA", 99000, 40, 24000),
    ("Scott", "Finance", "NY", 83000, 36, 19000),
    ("Jen", "Finance", "NY", 79000, 53, 15000),
    ("Jeff", "Marketing", "CA", 80000, 25, 18000),
    ("Kumar", "Marketing", "NY", 91000, 50, 21000)
]

# Generate 100 Random Records
departments = ["Sales", "Finance", "Marketing", "IT", "HR"]
states = ["NY", "CA", "TX", "FL", "IL"]

for i in range(100):
    name = f"Employee_{i+1}"
    department = random.choice(departments)
    state = random.choice(states)
    salary = random.randint(50000, 120000)  # Salary between 50,000 and 120,000
    age = random.randint(22, 60)           # Age between 22 and 60
    bonus = random.randint(5000, 30000)   # Bonus between 5,000 and 30,000
    simpleData.append((name, department, state, salary, age, bonus))

# Define Schema
schema = ["employee_name", "department", "state", "salary", "age", "bonus"]

# Create DataFrame
df = spark.createDataFrame(data=simpleData, schema=schema)

# Show the first 20 rows of the expanded DataFrame
print("Expanded DataFrame with 109 Records:")
df.show(20)

# Count total records to verify
print(f"Total Records: {df.count()}")

# COMMAND ----------

df.agg(avg("salary"), avg("age"), avg("bonus")).show()

# COMMAND ----------

df.groupBy("department").agg(avg("salary")).sort('avg(salary)').show()

# COMMAND ----------

num_emp_df = df.groupBy("department").count()
display(num_emp_df)

# COMMAND ----------

highest_salary_df = df.groupBy("department").agg(max("salary")).sort(max('salary'), ascending=False).show()

# COMMAND ----------

display(df.sort(col('salary').desc()).select("employee_name", "salary", "department").limit(1))

# COMMAND ----------

emp_Livingin_California_df = df.filter(df.state == "CA")
emp_Livingin_California_df.show()

# COMMAND ----------

display((df.select(col('employee_name'),col('state'))).where(col('state') == 'CA'))

# COMMAND ----------

df.where(col('state') == 'CA').show()

# COMMAND ----------

df.filter(col('salary') > 100000).sort(col('salary').desc()).show()

# COMMAND ----------

df.filter(col('department') == 'Marketing').filter(col('salary') > 0.1 * col('bonus')).show()

# COMMAND ----------

df.groupBy('department').agg(avg('salary').alias('avg_salary')).sort('avg_salary',ascending=False).show(1)

# COMMAND ----------

df.groupBy('department').agg(min('age')).sort(min('age')).show()

# COMMAND ----------

df.groupBy('state').count().show()


# COMMAND ----------

df.groupBy('state').agg(sum('salary')).sort(sum('salary')).show()

# COMMAND ----------



# COMMAND ----------

states = ['CA', 'NY', 'TX']
df.select('employee_name','department').where(col('state').isin(states)).show()

# COMMAND ----------

from pyspark.sql.functions import col

display(df.orderBy(col('salary').desc()).tail(5))

-- Databricks notebook source
CREATE TABLE students
  (id INT, name STRING, value DOUBLE);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS students 
  (id INT, name STRING, value DOUBLE)

-- COMMAND ----------

INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3)

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

UPDATE students 
SET value = value + 1
WHERE name LIKE "T%"

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("""
-- MAGIC DELETE FROM students 
-- MAGIC WHERE value > 6
-- MAGIC """)

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Using Merge
-- MAGIC
-- MAGIC Some SQL systems have the concept of an upsert, which allows updates, inserts, and other data manipulations to be run as a single command.
-- MAGIC
-- MAGIC Databricks uses the **`MERGE`** keyword to perform this operation.
-- MAGIC
-- MAGIC Consider the following temporary view, which contains 4 records that might be output by a Change Data Capture (CDC) feed.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
SELECT * FROM updates;

-- COMMAND ----------

MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC The query you provided performs a **MERGE operation** to synchronize the data between the `students` table (target) and the `updates` temporary view (source). Here's a detailed explanation of the query:
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### **Query Breakdown:**
-- MAGIC
-- MAGIC #### **1. `MERGE INTO students b USING updates u ON b.id=u.id`**
-- MAGIC    - **What it does**: This merges data from the `updates` view (`u`) into the `students` table (`b`), based on a condition.
-- MAGIC    - **Condition**: `ON b.id = u.id`
-- MAGIC      - Matches rows in `students` (`b`) and `updates` (`u`) where the `id` values are the same.
-- MAGIC
-- MAGIC #### **2. `WHEN MATCHED` Clauses**
-- MAGIC    - These clauses handle cases where a row in `students` has the same `id` as a row in `updates`.
-- MAGIC
-- MAGIC    - **First Clause:**
-- MAGIC      ```sql
-- MAGIC      WHEN MATCHED AND u.type = "update"
-- MAGIC        THEN UPDATE SET *
-- MAGIC      ```
-- MAGIC      - **Condition**: The rows match by `id`, and the `type` in `updates` is `"update"`.
-- MAGIC      - **Action**: Updates all columns (`*`) in `students` with the corresponding values from `updates`.
-- MAGIC
-- MAGIC    - **Second Clause:**
-- MAGIC      ```sql
-- MAGIC      WHEN MATCHED AND u.type = "delete"
-- MAGIC        THEN DELETE
-- MAGIC      ```
-- MAGIC      - **Condition**: The rows match by `id`, and the `type` in `updates` is `"delete"`.
-- MAGIC      - **Action**: Deletes the matching row from `students`.
-- MAGIC
-- MAGIC #### **3. `WHEN NOT MATCHED` Clause**
-- MAGIC    - **Condition**: There is no matching `id` in `students` for the `id` in `updates`, and the `type` in `updates` is `"insert"`.
-- MAGIC    - **Action**:
-- MAGIC      ```sql
-- MAGIC      WHEN NOT MATCHED AND u.type = "insert"
-- MAGIC        THEN INSERT *
-- MAGIC      ```
-- MAGIC      - Inserts a new row into `students` with all columns from `updates`.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### **Example Execution:**
-- MAGIC
-- MAGIC #### Initial `students` Table:
-- MAGIC | id  | name     | value  |
-- MAGIC |------|----------|--------|
-- MAGIC | 2    | Omar     | 2.5    |
-- MAGIC | 3    | Elia     | 3.3    |
-- MAGIC | 4    | Ted      | 4.7    |
-- MAGIC
-- MAGIC #### `updates` Temporary View:
-- MAGIC | id  | name   | value  | type    |
-- MAGIC |-----|--------|--------|---------|
-- MAGIC | 2   | Omar   | 15.2   | update  |
-- MAGIC | 3   |        | null   | delete  |
-- MAGIC | 7   | Blue   | 7.7    | insert  |
-- MAGIC | 11  | Diya   | 8.8    | update  |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### **Steps and Results After Merge:**
-- MAGIC
-- MAGIC 1. **`WHEN MATCHED AND u.type = "update"`**
-- MAGIC    - Row with `id = 2` is updated: `value = 15.2`.
-- MAGIC    - Row with `id = 11` is skipped as it doesn’t exist in `students`.
-- MAGIC
-- MAGIC 2. **`WHEN MATCHED AND u.type = "delete"`**
-- MAGIC    - Row with `id = 3` is deleted.
-- MAGIC
-- MAGIC 3. **`WHEN NOT MATCHED AND u.type = "insert"`**
-- MAGIC    - Row with `id = 7` is inserted.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC #### Final `students` Table:
-- MAGIC | id  | name     | value  |
-- MAGIC |-----|----------|--------|
-- MAGIC | 2   | Omar     | 15.2   |
-- MAGIC | 4   | Ted      | 4.7    |
-- MAGIC | 7   | Blue     | 7.7    |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### **Key Points:**
-- MAGIC
-- MAGIC - The `MERGE` operation simplifies conditional updates, inserts, and deletions in a single query.
-- MAGIC - The use of `SET *` and `INSERT *` updates/inserts all columns directly. This approach assumes column alignment between the target and source.
-- MAGIC - It's ideal for syncing data between tables or applying change data capture (CDC) updates.
-- MAGIC
-- MAGIC Let me know if you’d like further examples or explanations! 😊

-- COMMAND ----------

DROP TABLE students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

# Databricks notebook source
# MAGIC %md
# MAGIC ########I have around 9.5 years of IT experience, with 5.5+ years in Big Data technologies and Big Data Testing. I have worked extensively with Hadoop, Hive, Sqoop, Spark, and PySpark, and I specialize in ETL/DWH and Big Data testing.
# MAGIC ########In my last project, I worked on end-to-end data pipeline testing, which was built on the Hadoop Medallion Architecture. The architecture was divided into Raw (Bronze), Silver, and Gold layers, and I was responsible for end-to-end data validation, including data quality, transformation accuracy, and reconciliation across all layers.
# MAGIC ########We received data from multiple source systems such as SAP, CCS, HLS, and CSV files. Initially, the incoming files were landed in a landing path on the Edge Node. From there, the files were moved to HDFS using a file transfer job, also known as a data watcher job.
# MAGIC ########Once the data reached the Raw (Bronze) layer in Hadoop, I performed sanity and delta testing, which included:
# MAGIC •	File format validation
# MAGIC •	File naming convention checks
# MAGIC •	Zero-byte file validation
# MAGIC •	Duplicate file name checks
# MAGIC •	Different file names with duplicate records
# MAGIC After successful raw layer validation, a master scheduler job triggered downstream processing. Based on the upstream files, staging tables were created.
# MAGIC From staging, the data was transformed and loaded into the Silver layer, where fact and dimension tables were populated. I validated:
# MAGIC •	Source-to-target record counts
# MAGIC •	Business transformation logic
# MAGIC •	Data type and NULL handling
# MAGIC •	Duplicate records
# MAGIC The Gold layer acted as the consumption layer, where business views were created. These views were consumed by BI tools, visualization teams, and data science teams. I ensured the data was accurate, aggregated correctly, and aligned with business requirements.
# MAGIC For data governance and compliance, data in the Gold layer was archived and secured using service accounts, and access was controlled as per governance standards.
# MAGIC Overall, my role involved validating the complete data flow from source systems to consumption, ensuring high data quality, accuracy, and reliability across the Medallion Architecture.#####
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC  
# MAGIC  
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## 👨‍💻 Professional Summary
# MAGIC
# MAGIC I have around **9.5 years of IT experience**, with **5.5+ years in Big Data technologies and Big Data Testing**. I have worked extensively with **Hadoop, Hive, Sqoop, Spark, and PySpark**, and I specialize in **ETL/DWH and Big Data testing**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ Project Overview – Medallion Architecture
# MAGIC
# MAGIC In my **last project**, I worked on **end-to-end data pipeline testing**, which was built on the **Hadoop Medallion Architecture**.  
# MAGIC The architecture consisted of **Raw (Bronze), Silver, and Gold layers**, and I was responsible for **end-to-end data validation**, including **data quality, transformation accuracy, and reconciliation across all layers**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Data Ingestion Flow
# MAGIC
# MAGIC We received data from multiple source systems such as:
# MAGIC - **SAP**
# MAGIC - **CCS**
# MAGIC - **HLS**
# MAGIC - **CSV files**
# MAGIC
# MAGIC Initially, the incoming files were landed in a **landing path on the Edge Node**.  
# MAGIC From there, the files were moved to **HDFS** using a **file transfer job**, also known as a **data watcher job**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🥉 Raw (Bronze) Layer Validation
# MAGIC
# MAGIC Once the data reached the **Raw (Bronze) layer** in Hadoop, I performed **sanity and delta testing**, which included:
# MAGIC
# MAGIC - File format validation  
# MAGIC - File naming convention checks  
# MAGIC - Zero-byte file validation  
# MAGIC - Duplicate file name checks  
# MAGIC - Different file names with duplicate records  
# MAGIC
# MAGIC After successful raw layer validation, a **master scheduler job** triggered downstream processing.  
# MAGIC Based on the upstream files, **staging tables** were created.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🥈 Silver Layer Validation
# MAGIC
# MAGIC From the staging layer, the data was transformed and loaded into the **Silver layer**, where **fact and dimension tables** were populated.
# MAGIC
# MAGIC I validated:
# MAGIC - Source-to-target record counts  
# MAGIC - Business transformation logic  
# MAGIC - Data type and NULL handling  
# MAGIC - Duplicate records  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🥇 Gold Layer (Consumption Layer)
# MAGIC
# MAGIC The **Gold layer** acted as the **consumption layer**, where **business views** were created.  
# MAGIC These views were consumed by:
# MAGIC - BI tools  
# MAGIC - Visualization teams  
# MAGIC - Data science teams  
# MAGIC
# MAGIC I ensured that the data was **accurate, properly aggregated, and aligned with business requirements**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔐 Data Governance & Compliance
# MAGIC
# MAGIC For **data governance and compliance**, data in the Gold layer was:
# MAGIC - Archived using **service accounts**
# MAGIC - Secured with **controlled access**
# MAGIC - Managed as per governance standards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Overall Responsibility
# MAGIC
# MAGIC Overall, my role involved **validating the complete data flow from source systems to consumption**, ensuring **high data quality, accuracy, and reliability** across the **Medallion Architecture**.
# MAGIC

# Big Data Pipeline for Real-Time Healthcare Analytics with MIMIC-III

## 🚀 Project Overview
**Title:** Building a Big Data Pipeline for Real-Time Healthcare Analytics with MIMIC-III  
**Objective:** Design and implement a scalable pipeline for batch and real-time processing of the MIMIC-III dataset, enabling analytics such as length-of-stay predictions, patient monitoring, and readmission risk analysis.

---

## ✅ Progress to Date

1. **Environment Setup**
   - Pulled and launched the `Marcel-Jan/docker-hadoop-spark` Docker Compose image, bringing up Hadoop (NameNode, DataNode), Spark, and Hive services.
   - Verified HDFS is accessible (safe-mode toggled off; root directory usable).

2. **Data Preparation**
   - Downloaded MIMIC-III demo CSVs and placed them into Google Drive for Colab processing.
   - In Colab:  
     - Mounted Google Drive.  
     - Cleaned and imputed missing values in all key CSV files:
       - **admissions.csv** → computed `length_of_stay`, imputed timestamps, filled categorical nulls.
       - **patients.csv** → parsed `dob`/`dod`, filled `gender`/`expire_flag` nulls.
       - **icustays.csv** → computed `icu_length_of_stay`, imputed care-unit fields.
       - **labevents.csv** → parsed `charttime`, converted `valuenum` to numeric, filled units.
       - **diagnoses_icd.csv** → renamed `icd9_code` → `diagnosis_code`, handled missing sequence numbers.
     - Wrote out cleaned Parquet files using PyArrow with `use_deprecated_int96_timestamps=True` for Hive compatibility.

3. **HDFS Ingestion**
   - Mounted the cleaned Parquet directory into the NameNode container.
   - Disabled safe mode (`hdfs dfsadmin -safemode leave`).
   - Cleared old `/mimic3/parquet` data and uploaded new Parquet files into per-table directories:
     ```
     /mimic3/parquet/admissions/part-00000.parquet
     /mimic3/parquet/patients/part-00000.parquet
     …
     ```

4. **Hive Table Creation**
   - Dropped any previous Hive tables.
   - Created **EXTERNAL** Hive tables pointing at each directory, matching the cleaned schema:
     - `mimic3_admissions` (subject_id, hospital_admission_id, admittime, dischtime, length_of_stay, admission_type, …)  
     - `mimic3_patients` (subject_id, dob, dod, gender, expire_flag)  
     - `mimic3_icustays` (subject_id, hadm_id, icustay_id, intime, outtime, icu_length_of_stay, …)  
     - `mimic3_labevents` (subject_id, hadm_id, itemid, charttime, valuenum, valueuom)  
     - `mimic3_diagnoses_icd` (subject_id, hadm_id, seq_num, diagnosis_code)  
   - Verified that each table shows non-zero row counts in Hive.

---

## 🔜 Next Steps

1. **Batch Analytics (HiveQL)**
   - Write and save HiveQL scripts for:
     - Average length of stay per diagnosis.
     - ICU readmission distribution.
     - Mortality rates by gender/ethnicity.
   - Export results to CSV or HDFS for reporting.

2. **Advanced Processing with Spark**
   - Spin up a SparkSession with Hive support.
   - Read Hive tables into DataFrames and perform more complex transforms or ML feature engineering.
   - Persist feature tables back to HDFS/Hive.

3. **Real-Time Streaming**
   - Add Kafka & Zookeeper to Docker Compose.
   - Define Spark Streaming jobs to consume events (e.g., new lab measurements) from Kafka, write to HDFS.
   - Create real-time monitoring dashboards.

4. **Orchestration with Airflow**
   - Deploy Airflow to schedule:
     - Daily batch Hive jobs.
     - Spark ML jobs.
     - Health checks on streaming pipelines.

5. **Documentation & Presentation**
   - Draft ER and architecture diagrams.
   - Prepare user and developer guides.
   - Build a slide deck summarizing the solution, key findings, and next steps.

---

## 📁 Repository Structure


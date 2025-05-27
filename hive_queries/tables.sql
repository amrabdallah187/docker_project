-- 1. ADMISSIONS Table
CREATE EXTERNAL TABLE admissions (
    subject_id            INT,
    hospital_admission_id INT,
    admittime             TIMESTAMP,
    dischtime             TIMESTAMP,
    length_of_stay        INT,
    admission_type        STRING,
    insurance             STRING,
    ethnicity             STRING,
    hospital_expire_flag  STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/mimic3/parquet/admissions';

-- 2. PATIENTS Table
CREATE EXTERNAL TABLE patients (
    subject_id  INT,
    dob         TIMESTAMP,
    dod         TIMESTAMP,
    gender      STRING,
    expire_flag STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/mimic3/parquet/patients';

-- 3. ICUSTAYS Table
CREATE EXTERNAL TABLE icustays (
    subject_id         INT,
    hadm_id            INT,
    icustay_id         INT,
    intime             TIMESTAMP,
    outtime            TIMESTAMP,
    icu_length_of_stay INT,
    first_careunit     STRING,
    last_careunit      STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/mimic3/parquet/icustays';

-- 4. LABEVENTS Table
CREATE EXTERNAL TABLE labevents (
    subject_id INT,
    hadm_id    INT,
    itemid     INT,
    charttime  TIMESTAMP,
    valuenum   DOUBLE,
    valueuom   STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/mimic3/parquet/labevents';

-- 5. DIAGNOSES_ICD Table
CREATE EXTERNAL TABLE diagnoses_icd (
    subject_id     INT,
    hadm_id        INT,
    seq_num        INT,
    diagnosis_code STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/mimic3/parquet/diagnoses_icd';

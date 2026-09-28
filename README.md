# ❄️ Snowflake Customer Data Ingestion & Governance Pipeline

> **An end-to-end Snowflake data engineering project that demonstrates automated data ingestion, CDC processing, state-wise data transformation, dynamic tables, and role-based data security.**

<p align="center">

![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=for-the-badge\&logo=snowflake\&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Advanced-336791?style=for-the-badge\&logo=postgresql\&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-Blob%20Storage-0078D4?style=for-the-badge\&logo=microsoftazure\&logoColor=white)
![Data Engineering](https://img.shields.io/badge/Data%20Engineering-ETL%2FELT-orange?style=for-the-badge)

</p>

---

## 📌 Table of Contents

<details>
<summary><b>Click to expand</b></summary>

* [Project Overview](#-project-overview)
* [Architecture](#-architecture)
* [Project Workflow](#-project-workflow)
* [Technologies Used](#-technologies-used)
* [Project Components](#-project-components)
* [Database Structure](#-database-structure)
* [Data Pipeline](#-data-pipeline)
* [Change Data Capture](#-change-data-capture)
* [Dynamic Tables](#-dynamic-tables)
* [Security & Governance](#-security--governance)
* [Project Setup](#-project-setup)
* [Testing](#-testing)
* [Key SQL Concepts](#-key-sql-concepts)
* [What I Learned](#-what-i-learned)
* [Future Enhancements](#-future-enhancements)

</details>

---

# 🚀 Project Overview

This project demonstrates an end-to-end **customer data ingestion and governance pipeline using Snowflake**.

Customer data is stored as files in **Azure Blob Storage**. Snowpipe is used to automatically ingest new files into the Snowflake **RAW layer**.

The ingested data is then monitored using **Snowflake Streams**, while **Tasks** automate downstream processing into state-specific curated tables.

Dynamic Tables are used to create city-level datasets, while **RBAC and Row Access Policies** provide role-based access to customer information.

### 🎯 Main Objectives

* Automate file ingestion into Snowflake
* Implement Change Data Capture (CDC)
* Create curated state-wise datasets
* Build city-wise Dynamic Tables
* Implement role-based access control
* Restrict data using Row Access Policies
* Demonstrate Snowflake data engineering capabilities

---

# 🏗️ Architecture

```text
                    ┌──────────────────────┐
                    │   Azure Blob Storage │
                    │                      │
                    │ Customer CSV Files   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      Snowpipe        │
                    │                      │
                    │ Automated Ingestion  │
                    └──────────┬───────────┘
                               │
                               ▼
              ┌────────────────────────────────┐
              │          RAW Layer             │
              │                                │
              │      RAW.CUSTOMERS             │
              └───────────────┬────────────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ Snowflake Stream  │
                    │                   │
                    │ CDC Detection     │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ Snowflake Task    │
                    │                   │
                    │ MERGE / Process   │
                    └─────────┬─────────┘
                              │
              ┌───────────────┴────────────────┐
              ▼                                ▼
     ┌─────────────────┐              ┌─────────────────┐
     │ NSW Customers   │              │ QLD Customers   │
     │ CURATED Layer   │              │ CURATED Layer   │
     └────────┬────────┘              └────────┬────────┘
              │                                │
              ▼                                ▼
       ┌─────────────┐                  ┌─────────────┐
       │ Dynamic     │                  │ Dynamic     │
       │ Tables      │                  │ Tables      │
       └──────┬──────┘                  └──────┬──────┘
              │                                │
              └──────────────┬─────────────────┘
                             ▼
                  ┌──────────────────────┐
                  │ Security & Governance│
                  │                      │
                  │ RBAC + Row Access    │
                  │ Policies             │
                  └──────────────────────┘
```

---

# 🔄 Project Workflow

```text
Azure Blob Storage
        │
        ▼
     Snowpipe
        │
        ▼
   RAW.CUSTOMERS
        │
        ▼
  STREAM (CDC)
        │
        ▼
     TASK
        │
        ▼
 ┌──────┴────────┐
 ▼               ▼
NSW Customers   QLD Customers
 │               │
 ▼               ▼
Dynamic Tables
 │
 ▼
City-wise Data
 │
 ▼
RBAC + Row Access Policy
```

---

# 🛠️ Technologies Used

| Technology             | Purpose                                   |
| ---------------------- | ----------------------------------------- |
| **Snowflake**          | Cloud data warehouse                      |
| **Snowpipe**           | Automated data ingestion                  |
| **Snowflake Streams**  | Change Data Capture                       |
| **Snowflake Tasks**    | Pipeline automation                       |
| **Dynamic Tables**     | Automatically refreshed analytical tables |
| **SQL**                | Data processing and transformation        |
| **Azure Blob Storage** | External data source                      |
| **RBAC**               | Role-based security                       |
| **Row Access Policy**  | Row-level data security                   |

---

# 🗂️ Database Structure

```text
CUSTOMER_DB
│
├── RAW
│   ├── CUSTOMERS
│   ├── CUSTOMER_PIPE_FORMAT
│   ├── EXT_STAGE
│   └── STR_CUSTOMERS
│
├── CURATED
│   ├── NSW_CUSTOMERS
│   └── QUEENSLAND_CUSTOMERS
│
└── DT
    ├── DT_SYDNEY
    ├── DT_NEWCASTLE
    ├── DT_BRISBANE
    └── DT_GOLD_COAST
```

---

# 📥 Data Ingestion

Customer files are stored in **Azure Blob Storage** using a pipe-delimited CSV format.

Example:

```text
CUSTOMER_ID|CUSTOMER_NAME|CUSTOMER_EMAIL|CUSTOMER_CITY|CUSTOMER_STATE|CUSTOMER_DOB
1001|John Smith|john@gmail.com|Sydney|NSW|1998-04-12
1002|David Lee|david@gmail.com|Newcastle|NSW|1995-08-21
1003|Michael Brown|michael@gmail.com|Brisbane|QLD|1997-01-15
```

A Snowflake **External Stage** is created to access the Azure Blob Storage location.

Snowpipe then loads newly available files into:

```sql
CUSTOMER_DB.RAW.CUSTOMERS
```

---

# ❄️ Snowpipe

Snowpipe is used for automated/incremental ingestion.

Basic structure:

```sql
CREATE OR REPLACE PIPE CUSTOMER_PIPE
AS
COPY INTO CUSTOMER_DB.RAW.CUSTOMERS
FROM @CUSTOMER_DB.RAW.EXT_STAGE
FILE_FORMAT = (
    FORMAT_NAME = 'CUSTOMER_PIPE_FORMAT'
);
```

For testing, the pipe can be refreshed manually:

```sql
ALTER PIPE CUSTOMER_PIPE REFRESH;
```

Check the pipe:

```sql
SHOW PIPES;
```

---

# 🔄 Change Data Capture

A Snowflake **Stream** tracks changes occurring in the RAW customer table.

```sql
CREATE OR REPLACE STREAM STR_CUSTOMERS
ON TABLE RAW.CUSTOMERS;
```

The stream captures information that can be consumed by downstream processing.

Example:

```sql
SELECT *
FROM RAW.STR_CUSTOMERS;
```

---

# ⚙️ Automated Processing

Snowflake Tasks are used to process the CDC data.

The task checks whether the Stream contains new data:

```sql
WHEN SYSTEM$STREAM_HAS_DATA(
    'CUSTOMER_DB.RAW.STR_CUSTOMERS'
)
```

The captured records are then processed using `MERGE`.

Example:

```sql
MERGE INTO CURATED.NSW_CUSTOMERS T
USING RAW.STR_CUSTOMERS S
ON T.CUSTOMER_ID = S.CUSTOMER_ID
```

This allows existing records to be updated and new records to be inserted.

---

# 🗃️ Curated Layer

Customer records are separated based on their state.

### NSW

```text
NSW
New South Wales
```

Stored in:

```sql
CURATED.NSW_CUSTOMERS
```

### Queensland

```text
QLD
Queensland
```

Stored in:

```sql
CURATED.QUEENSLAND_CUSTOMERS
```

---

# 🔄 Dynamic Tables

Dynamic Tables are created to provide city-specific datasets.

### Sydney

```sql
CREATE OR REPLACE DYNAMIC TABLE DT_SYDNEY
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
SELECT *
FROM CURATED.NSW_CUSTOMERS
WHERE CUSTOMER_CITY = 'Sydney';
```

### Newcastle

```sql
CREATE OR REPLACE DYNAMIC TABLE DT_NEWCASTLE
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
SELECT *
FROM CURATED.NSW_CUSTOMERS
WHERE CUSTOMER_CITY = 'Newcastle';
```

### Brisbane

```sql
CREATE OR REPLACE DYNAMIC TABLE DT_BRISBANE
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
SELECT *
FROM CURATED.QUEENSLAND_CUSTOMERS
WHERE CUSTOMER_CITY = 'Brisbane';
```

### Gold Coast

```sql
CREATE OR REPLACE DYNAMIC TABLE DT_GOLD_COAST
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
SELECT *
FROM CURATED.QUEENSLAND_CUSTOMERS
WHERE CUSTOMER_CITY = 'Gold Coast';
```

---

# 🔐 Security & Governance

The project implements **Role-Based Access Control (RBAC)**.

Two roles are created:

```text
NSW_MANAGER
QLD_MANAGER
```

Users:

```text
NSW_USER
QLD_USER
```

The roles are granted access to the Snowflake environment.

---

# 🛡️ Row Access Policy

A Row Access Policy is used to restrict customer records based on the user's role.

```sql
CREATE OR REPLACE ROW ACCESS POLICY STATE_POLICY
AS (CUSTOMER_STATE STRING)

RETURNS BOOLEAN ->
CASE

WHEN CURRENT_ROLE() = 'ACCOUNTADMIN'
THEN TRUE

WHEN CURRENT_ROLE() = 'NSW_MANAGER'
THEN CUSTOMER_STATE IN ('NSW', 'New South Wales')

WHEN CURRENT_ROLE() = 'QLD_MANAGER'
THEN CUSTOMER_STATE IN ('QLD', 'Queensland')

ELSE FALSE

END;
```

This means:

```text
NSW_MANAGER
      │
      ▼
NSW Customers only


QLD_MANAGER
      │
      ▼
QLD Customers only
```

---

# 🧪 Testing

### Check RAW data

```sql
SELECT *
FROM CUSTOMER_DB.RAW.CUSTOMERS;
```

### Check Stream

```sql
SELECT *
FROM CUSTOMER_DB.RAW.STR_CUSTOMERS;
```

### Check NSW customers

```sql
SELECT *
FROM CUSTOMER_DB.CURATED.NSW_CUSTOMERS;
```

### Check Queensland customers

```sql
SELECT *
FROM CUSTOMER_DB.CURATED.QUEENSLAND_CUSTOMERS;
```

### Check Dynamic Tables

```sql
SELECT *
FROM CUSTOMER_DB.DT.DT_SYDNEY;

SELECT *
FROM CUSTOMER_DB.DT.DT_NEWCASTLE;

SELECT *
FROM CUSTOMER_DB.DT.DT_BRISBANE;

SELECT *
FROM CUSTOMER_DB.DT.DT_GOLD_COAST;
```

### Check Row Access Policies

```sql
SHOW ROW ACCESS POLICIES;
```

---

# 📊 Example Data Flow

```text
customer_batch_01.csv
          │
          ▼
   Azure Blob Storage
          │
          ▼
       Snowpipe
          │
          ▼
     RAW.CUSTOMERS
          │
          ▼
    STR_CUSTOMERS
          │
          ▼
   TASK_MERGE_CUSTOMERS
          │
     ┌────┴─────┐
     ▼          ▼
    NSW         QLD
     │          │
     ▼          ▼
 CURATED     CURATED
     │          │
     └────┬─────┘
          ▼
    Dynamic Tables
          │
          ▼
  City-wise datasets
          │
          ▼
 RBAC + Row Security
```

---

# 📁 Suggested Repository Structure

```text
Snowflake-Customer-Data-Pipeline/
│
├── README.md
│
├── sql/
│   ├── 01_database_schema.sql
│   ├── 02_raw_layer.sql
│   ├── 03_stage_file_format.sql
│   ├── 04_snowpipe.sql
│   ├── 05_stream.sql
│   ├── 06_curated_layer.sql
│   ├── 07_tasks.sql
│   ├── 08_dynamic_tables.sql
│   ├── 09_rbac.sql
│   ├── 10_row_access_policy.sql
│   └── 11_testing.sql
│
├── data/
│   └── sample_customers.csv
│
└── screenshots/
    ├── snowpipe.png
    ├── stream.png
    ├── dynamic_tables.png
    └── row_access_policy.png
```

---

# ▶️ Project Setup

### 1. Clone the repository

```bash
git clone https://github.com/Vaikash7/Snowflake-Customer-Data-Pipeline.git
```

### 2. Open Snowflake

Create a Snowflake worksheet and execute the SQL scripts in order.

### 3. Configure Azure Storage

Create an Azure Blob Storage container and upload the customer CSV files.

### 4. Configure the External Stage

Update the stage configuration with your own Azure Storage details.

> ⚠️ **Security:** Never commit Azure SAS tokens, passwords, access keys, or other credentials to GitHub.

### 5. Execute the Snowpipe

```sql
ALTER PIPE CUSTOMER_PIPE REFRESH;
```

### 6. Verify the pipeline

```sql
SELECT *
FROM CUSTOMER_DB.RAW.CUSTOMERS;
```

---

# 🧠 Key Snowflake Concepts Demonstrated

* Database & Schema creation
* External Stages
* File Formats
* Azure Blob Storage integration
* Snowpipe
* COPY INTO
* Streams
* Change Data Capture
* Tasks
* MERGE operations
* Dynamic Tables
* RBAC
* Roles & Users
* Row Access Policies
* Data Governance
* Incremental Data Processing

---

# 💡 What I Learned

Through this project, I gained practical experience in:

* Designing Snowflake database architectures
* Building automated data ingestion pipelines
* Working with Azure Blob Storage
* Implementing CDC using Snowflake Streams
* Automating transformations using Tasks
* Creating Dynamic Tables
* Implementing role-based security
* Applying row-level data access restrictions
* Understanding Snowflake's data engineering and governance capabilities

---

# 🔮 Future Enhancements

Potential improvements include:

* [ ] Enable Azure Event Grid notifications for true Snowpipe auto-ingestion
* [ ] Add data quality validation
* [ ] Add rejected-record handling
* [ ] Add pipeline monitoring and logging
* [ ] Add historical tracking using SCD Type 2
* [ ] Add Snowflake dashboards
* [ ] Add CI/CD deployment using GitHub Actions
* [ ] Add automated testing for SQL transformations

---

# 👨‍💻 Author

**Chatrathi Vaikash**

🎓 B.Tech – Computer Science & Engineering
💻 Data Engineering | Snowflake | Azure | PySpark | SQL

### Connect with me

[![GitHub](https://img.shields.io/badge/GitHub-Vaikash7-181717?style=for-the-badge\&logo=github)](https://github.com/Vaikash7)

---

⭐ **If you found this project useful, consider giving the repository a star!**

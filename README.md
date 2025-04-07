# DEOps_Project

## Overview

The **DEOps_Project** is a data engineering project focused on building and operating a data pipeline for the Vietnam stock market. It collects, processes, and stores stock market data using the `vnstock` library. The project supports data extraction, transformation, and loading (ETL) into a database for further analysis. The setup is fully containerized, leveraging modern technologies for scalability, flexibility, and maintainability.

---

## Features

### Data Extraction
- Fetches stock market data from the `vnstock` API.
- Collects company information and detailed profiles.

### Data Transformation
- Processes and cleans the data into structured formats (CSV and JSON).

### Data Loading
- Loads processed data into a relational database for further usage.

### Technologies Applied
- **Python**: For scripting and data processing.
- **Docker**: For containerization and portability.
- **PostgreSQL**: For storing and querying processed data.
- **Docker Compose**: For managing multi-container Docker applications.

---

## Directory Structure

```
DEOps_Project/
├── data/                        # Directory for storing output files
├── logs/                        # Directory for storing log files
│   ├── error_log.txt            # Log file for error messages
├── src/                         # Source code directory
│   ├── data_utils.py            # Library containing reusable functions
│   ├── other python files       # Script for fetching all data
├── .env                         # Environment variables file
├── Dockerfile                   # Dockerfile for building the container
├── docker-compose.yml           # Docker Compose configuration file
├── .gitignore                   # Git ignore file
├── README.md                    # Project documentation
```

---

## Prerequisites

- **Python**: Version 3.9 or later.
- **Docker**: Docker and Docker Compose installed on your system.
- **Database**: A running PostgreSQL database (or any other database of your choice).

---

## Setup and Usage

### Step 1: Clone the Repository
```bash
git clone <repository-url>
cd VNStock
```

### Step 2: Create the `.env` File
Create a `.env` file in the root directory with the following content:
```
DATA_DIR=/app/data
LOG_DIR=/app/logs
ERROR_LOG_FILE=${LOG_DIR}/error.txt
COMPANIES_FILE=${DATA_DIR}/companies.csv
COMPANY_INFO_FILE=${DATA_DIR}/company_info.json
COMPANY_OFFICER_FILE=${DATA_DIR}/comapny_officers.csv
COMPANY_SHAREHOLDER_FILE=${DATA_DIR}/company_shareholders.csv
COMPANY_DIVIDENDS_FILE=${DATA_DIR}/company_dividends.csv
COMPANY_STOCK_QUOTE_FILE=${DATA_DIR}/company_stock_quote.csv
COMPANY_INCOME_STATEMENT_FILE=${DATA_DIR}/income_statement.csv
COMPANY_BALANCE_SHEET_FILE=${DATA_DIR}/balance_sheet.csv
COMPANY_CASH_FLOW_FILE=${DATA_DIR}/cash_flow.csv
COMPANY_RATIO_FILE=${DATA_DIR}/ratio.csv
IS_TEST=True
```

### Step 3: Build and Run the Project

#### Using Docker Compose
Build and run the container:
```bash
docker-compose up --build
```

If you want to set the `IS_TEST` parameter dynamically, you can modify the `.env` file or set it using an environment variable:
```bash
set IS_TEST=False
docker-compose up -d
```

#### Without Docker Compose
Build the Docker image:
```bash
docker build -t deops_project .
```
Run the container:
```bash
docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/logs:/app/logs deops_project
```

---


## Acknowledgments

- [vnstock](https://pypi.org/project/vnstock/) for providing the stock market API.
- [psycopg2](https://pypi.org/project/psycopg2/) for PostgreSQL database integration.
- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) for containerization.
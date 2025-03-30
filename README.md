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

---

## Directory Structure

```
DEOps_Project/
├── data/                  # Directory for storing output files
│   ├── companies.csv      # Generated CSV file with company data
│   ├── company_info.json  # Generated JSON file with detailed company info
├── src/                   # Source code directory
│   ├── data_crawler.py    # Main script for fetching and processing data
│   ├── test.py            # Script for testing environment variables and data
├── .env                   # Environment variables file
├── Dockerfile             # Dockerfile for building the container
├── docker-compose.yml     # Docker Compose configuration file
├── .gitignore             # Git ignore file
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
cd DEOps_Project
```

### Step 2: Create the `.env` File
Create a `.env` file in the root directory with the following content:
```
DATA_DIR=../data
COMPANIES_FILE=${DATA_DIR}/companies.csv
COMPANY_INFO_FILE=${DATA_DIR}/company_info.json
```

---
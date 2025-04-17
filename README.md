# DEOps_Project

## Overview

The **DEOps_Project** is a data engineering project focused on building and operating a data pipeline for the Vietnam stock market. It collects, processes, and stores stock market data using the `vnstock` library. The project supports data extraction, transformation, and loading (ETL) into a database for further analysis.

The architecture leverages **Google Cloud Storage (GCS)** as the data lake for storing raw and cleaned data, **Apache Spark** for data standardization, and **BigQuery** as the database for querying and analyzing the processed data. The setup is fully containerized, leveraging modern technologies for scalability, flexibility, and maintainability.

---

## Features

### Data Extraction
- Fetches stock market data from the `vnstock` API.
- Stores raw data directly into GCS.

### Data Transformation
- Uses Apache Spark to clean and standardize raw data stored in GCS.
- Outputs cleaned data back to GCS.

### Data Loading
- Loads cleaned data from GCS into BigQuery for further analysis.

### Technologies Applied
- **Python**: For scripting and data processing.
- **Google Cloud Storage (GCS)**: Acts as the data lake for raw and cleaned data.
- **Apache Spark**: For data cleaning and standardization.
- **BigQuery**: For storing and querying processed data.
- **Docker**: For containerization and portability.
- **Docker Compose**: For managing multi-container Docker applications.

---

## Directory Structure

```
DEOps_Project/
├── src/                         # Source code directory
│   ├── data_crawler/            # Handles data extraction and raw file uploads to GCS
│   │   ├── service files        # Scripts for fetching data (e.g., companies, officers, etc.)
│   │   ├── util files           # Utilities for interacting with GCS and VNStock API
│   ├── data_cleaner/            # Handles data cleaning and standardization
│   │   ├── service files        # Scripts for fetching data (e.g., companies, officers, etc.)
│   │   ├── util files           # Utilities for interacting with GCS and spark session
├── .env                         # Environment variables file
├── Dockerfile                   # Dockerfile for building the container
├── docker-compose.yml           # Docker Compose configuration file
├── .dockerignore                # Docker ignore file
├── gcs_credentials.json         # Authentication file for GCS
├── requirements.txt             # Python dependencies
├── .gitignore                   # Git ignore file
├── README.md                    # Project documentation
```

---

## Prerequisites

- **Python**: Version 3.10
- **Docker**: Docker and Docker Compose installed on your system.
- **Google Cloud SDK**: Installed and authenticated with access to your GCS bucket and BigQuery project.
- **BigQuery**: A BigQuery dataset and table for storing processed data.
- **Apache Spark**: Installed and configured for local or cluster-based execution.

---

## Setup and Usage

### Step 1: Clone the Repository
```bash
git clone <repository-url>
cd VNStock
```

### Step 2: Configure Environment Variables
Create a `.env` file in the root directory with the following content:
```
GCS_CREDENTIALS=/app/gcs_credentials.json
GCS_BUCKET=your-gcs-bucket-name
LOG_DIR=/app/logs
ERROR_LOG_FILE=${LOG_DIR}/error.txt
IS_TEST=True
```

**Note**: You must manually add your Google Cloud service account credential file (`gcs_credentials.json`) to the root directory of the project. This file is required for authenticating with Google Cloud Storage

### Step 3: Install Dependencies
Install the required Python dependencies using `requirements.txt`:
```bash
pip install -r requirements.txt
```

### Step 4: Build and Run the Project

#### Build the Docker Image
To build the Docker image, use the following command:
```bash
docker build -t vnstock:<tag-version> .
```
Replace `<tag-version>` with the desired version tag (e.g., `1.0`).

#### Run the Services Using Docker Compose
To run specific services using Docker Compose, use:
```bash
docker-compose up -d <service-name>
```
Replace `<service-name>` with the name of the service you want to run (e.g., `company-info`, `financial-data`).

#### Example Commands
- Build the image with version `1.0`:
  ```bash
  docker build -t vnstock:1.0 .
  ```

- Run the `company-info` service:
  ```bash
  docker-compose up -d company-info
  ```

- Stop a specific service:
  ```bash
  docker-compose stop <service-name>
  ```

- View logs for a specific service:
  ```bash
  docker-compose logs <service-name>
  ```

---

## Workflow

1. **Data Extraction**:
   - The `data_crawler` scripts fetch raw data from the `vnstock` API and upload it directly to GCS.

2. **Data Cleaning**:
   - The `data_cleaner` scripts use Apache Spark to clean and standardize the raw data stored in GCS.
   - Cleaned data is written back to GCS in a separate folder.

3. **Data Loading**:
   - Cleaned data from GCS is loaded into BigQuery for further analysis.

---

## Acknowledgments

- [vnstock](https://pypi.org/project/vnstock/) for providing the stock market API.
- [Google Cloud Storage](https://cloud.google.com/storage) for acting as the data lake.
- [Apache Spark](https://spark.apache.org/) for data cleaning and standardization.
- [BigQuery](https://cloud.google.com/bigquery) for storing and querying processed data.
- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) for containerization.
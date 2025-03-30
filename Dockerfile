FROM python:3.9

# Install dependencies
RUN pip install vnstock pandas python-dotenv

# Set the working directory
WORKDIR /app

# Copy the application files
COPY data_crawler.py .
COPY .env .

# Run the script
CMD ["python", "data_crawler.py"]
FROM python:3.9

# Install dependencies
RUN pip install vnstock pandas python-dotenv

# Set the working directory
WORKDIR /app

# Copy the application files
COPY src/data_utils.py .
COPY src/main.py .
COPY .env .

# Run the script
ENTRYPOINT ["python", "main.py"]
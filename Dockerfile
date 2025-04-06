FROM python:3.9

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR /app

# Copy the application files
COPY src/ ./src
COPY .env .

# Run the script
ENTRYPOINT ["python"]
CMD ["/bin/bash"]
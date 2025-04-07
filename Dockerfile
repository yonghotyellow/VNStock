FROM python:3.10

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR /app

RUN mkdir -p data logs

# Copy the application files
COPY src/ ./src
COPY .env .

# Run the script
# ENTRYPOINT ["python"]
CMD ["/bin/bash"]
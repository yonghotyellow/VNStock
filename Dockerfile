FROM python:3.10-slim

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR /app

RUN mkdir -p logs 
RUN touch logs/error.txt

# Copy source code and environment file
COPY src/ ./src
COPY .env .

# Default CMD can be overridden by docker-compose `command`
CMD ["/bin/bash"]

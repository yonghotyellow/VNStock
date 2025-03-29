FROM python:3.9

RUN pip install vnstock pandas

# Tạo thư mục trong container
WORKDIR /app

COPY data_crawler.py .

CMD ["python", "fetch_vn30.py"]

FROM python:3.9

RUN pip install vnstock pandas

WORKDIR /app

COPY data_crawler.py .

CMD ["python", "data_crawler.py"]

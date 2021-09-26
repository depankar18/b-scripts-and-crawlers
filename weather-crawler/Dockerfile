FROM python:3.8-alpine
WORKDIR /weather_crawler
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "./crawler.py"]
FROM python:3.8-slim
WORKDIR /app
COPY . /app
RUN pip install pandas numpy
CMD ["python", "./script.py"]

FROM python:3.6-slim  

WORKDIR /app           

COPY . /app


RUN apt-get update
RUN pip install --upgrade pip
RUN pip install --trusted-host pypi.python.org -r requirements.txt


CMD ["python", "etl.py"]

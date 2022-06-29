FROM tiangolo/uwsgi-nginx-flask:python3.9
RUN apt-get update && apt-get install bash make automake gcc g++ python3-dev
RUN pip install --upgrade pip
COPY requirements.txt /tmp/requirements.txt
COPY app/ /app/
RUN pip install --no-cache-dir -r /tmp/requirements.txt


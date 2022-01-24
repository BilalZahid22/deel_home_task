FROM postgres
ENV POSTGRES_PASSWORD 1122
ENV POSTGRES_DB Testing

# Not required in task
#FROM python:latest
#WORKDIR /usr/app/src
#COPY . ./
#RUN pip install -r requirements.txt
#CMD [ 'python3', '-u', './main.py']

FROM python:3.9.6-buster

WORKDIR    /opt/oracle
RUN        apt-get update && apt-get install -y libaio1 wget unzip \
            && wget https://download.oracle.com/otn_software/linux/instantclient/213000/instantclient-basic-linux.x64-21.3.0.0.0.zip \
            && unzip instantclient-basic-linux.x64-21.3.0.0.0.zip \
            && rm -f instantclient-basic-linux.x64-21.3.0.0.0.zip \
            && cd /opt/oracle/instantclient* \
            && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci \
            && echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf \
            && ldconfig

WORKDIR /app
ADD . .
RUN pip install cx_Oracle
RUN pip install cacheout
RUN pip install fastapi

RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install


EXPOSE 8090
CMD ["uvicorn","dqc.main:app","--host", "0.0.0.0", "--port", "8090"]







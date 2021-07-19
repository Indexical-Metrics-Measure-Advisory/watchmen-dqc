FROM python:3.9

WORKDIR    /opt/oracle
RUN        apt-get update && apt-get install -y libaio1 wget unzip \
            && wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip \
            && unzip instantclient-basiclite-linuxx64.zip \
            && rm -f instantclient-basiclite-linuxx64.zip \
            && cd /opt/oracle/instantclient* \
            && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci \
            && echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf \
            && ldconfig

WORKDIR /app
ADD . .
RUN pip install cx_Oracle
RUN pip install cacheout
RUN pip install fastapi
# RUN pip install scikit-learn
# RUN pip install great-expectations
# RUN pip install pandas
# RUN pip install fastapi
# RUN pip install python-dotenv
# RUN pip install trino
# RUN pip install pandas-profiling
# RUN pip install uvicorn
# RUN pip install prophet
# RUN pip install plotly
# RUN pip install APScheduler
# RUN pip install arrow


RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install


EXPOSE 8000
CMD ["uvicorn","dqc.main:app","--host", "0.0.0.0", "--port", "8090"]







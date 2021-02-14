FROM python:3.8

WORKDIR /usr/src/app

COPY docker/build/etl/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# COPY runtime .
# COPY data ./data

ENTRYPOINT [ "python", "entrypoint.py" ]
# CMD [ "-a", "start" ]
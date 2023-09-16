FROM python:3.9

WORKDIR /code

RUN pip install --no-cache-dir --upgrade aioarango pandas appengine-python-standard

COPY ./db_import.py /code/db_import.py

CMD ["python", "db_import.py"]

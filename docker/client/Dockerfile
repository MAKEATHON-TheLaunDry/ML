FROM python:3.9

COPY ../../requirements.txt .

RUN pip install -r requirements.txt

COPY ../../utils.py .
COPY ../../client.py .
COPY ../../db_import.py .

CMD ["python", "client.py"]

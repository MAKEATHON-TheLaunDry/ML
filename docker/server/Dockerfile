FROM python:3.9

COPY ../../requirements.txt .

RUN pip install -r requirements.txt

COPY ../../utils.py .
COPY ../../server.py .

CMD ["python", "server.py"]

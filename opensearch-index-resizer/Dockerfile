FROM python:3-slim-stretch

COPY . /app

RUN pip install -qr /app/requirements.txt

ENTRYPOINT ["python", "/app/application.py"]

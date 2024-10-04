FROM python:3.11-bookworm
WORKDIR /src

COPY requirements.txt .
COPY app/ ./app/

RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 8079

CMD ["uvicorn", "app.main:app", "--port", "8079", "--host", "0.0.0.0"]

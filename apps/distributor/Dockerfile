FROM python:3.13-slim

WORKDIR /app
COPY shared /app/shared
COPY apps/receiver /app

RUN pip install uv && uv pip install -r requirements.txt

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0"]
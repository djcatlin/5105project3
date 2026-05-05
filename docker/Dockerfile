FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN python -m grpc_tools.protoc -I /app/proto --python_out=/app/proto/src --pyi_out=/app/proto/src --grpc_python_out=/app/proto/src /app/proto/project3.proto

ENV PYTHONPATH=/app:/app/proto/src
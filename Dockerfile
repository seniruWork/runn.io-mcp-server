FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY mcp_runn_server.py runn_reports.py ./

EXPOSE 8000

CMD ["python", "mcp_runn_server.py", "--transport", "streamable-http"]

FROM python:3.13-slim

WORKDIR /app

COPY . .

EXPOSE 3000

RUN pip install --no-cache-dir fastapi uvicorn psycopg[binary]

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3000"]

HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:3000/health').status==200 else 1)" || exit 1

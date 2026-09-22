# Standard library only, so no requirements file.
FROM python:3.12-slim

WORKDIR /app
COPY fake_salesforce/ ./fake_salesforce/

ENV PYTHONUNBUFFERED=1 PORT=8080
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/__admin/health')"

USER nobody
CMD ["python", "-m", "fake_salesforce"]

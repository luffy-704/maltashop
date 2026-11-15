FROM python:3.12-slim AS build
WORKDIR /w
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim
RUN useradd -r -u 1001 app && mkdir /data && chown app /data
COPY --from=build /install /usr/local
WORKDIR /srv
COPY app.py .
USER app
EXPOSE 8000
HEALTHCHECK --interval=10s --retries=3 CMD python -c "import urllib.request as u; u.urlopen('http://localhost:8000/health')"
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.12-slim-bookworm AS parser-build
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*
COPY pipeline/native/ /build/native/
RUN python /build/native/build.py

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY pipeline/ /app/pipeline/
COPY --from=parser-build /build/_fastparse*.so /app/pipeline/
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 1000:1000
CMD ["/app/run.sh"]

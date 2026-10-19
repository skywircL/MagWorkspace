FROM python:3.13-alpine
RUN addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY bridges/magent/requirements.txt bridges/magent/service_requirements.txt /app/bridges/magent/
RUN python3 -m pip install --no-cache-dir --require-hashes --only-binary=:all: \
    --target /app/bridges/magent/.deps -r /app/bridges/magent/service_requirements.txt
COPY bridges /app/bridges
USER app
EXPOSE 8090
ENTRYPOINT ["python3", "-S", "-B", "/app/bridges/magent/service.py"]

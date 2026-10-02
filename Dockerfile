FROM python:3.12-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless procps bash nano curl && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-home

ENV JAVA_HOME=/opt/java-home
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN mkdir -p /app/output /app/scripts /app/dashboard

EXPOSE 4040 8501
CMD ["bash"]

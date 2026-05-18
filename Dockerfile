FROM eclipse-temurin:8-jdk-jammy

ARG HADOOP_VERSION=3.4.3
ARG PIG_VERSION=0.17.0
ARG HIVE_VERSION=3.1.3

ENV APP_HOME=/app \
    HADOOP_HOME=/opt/hadoop \
    PIG_HOME=/opt/pig \
    HIVE_HOME=/opt/hive \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HIVE_CONF_DIR=/opt/hive/conf \
    JAVA_HOME=/opt/java/openjdk

ENV PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin:$HIVE_HOME/bin:$PATH"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        dos2unix \
        netcat-openbsd \
        nodejs \
        npm \
        postgresql-client \
        procps \
        python3 \
        python3-pip \
        tar \
        wget \
    && rm -rf /var/lib/apt/lists/*

RUN wget "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -O /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv "/opt/hadoop-${HADOOP_VERSION}" "$HADOOP_HOME" \
    && rm /tmp/hadoop.tar.gz

RUN wget "https://dlcdn.apache.org/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz" -O /tmp/pig.tar.gz \
    && tar -xzf /tmp/pig.tar.gz -C /opt \
    && mv "/opt/pig-${PIG_VERSION}" "$PIG_HOME" \
    && rm /tmp/pig.tar.gz

RUN wget "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" -O /tmp/hive.tar.gz \
    && tar -xzf /tmp/hive.tar.gz -C /opt \
    && mv "/opt/apache-hive-${HIVE_VERSION}-bin" "$HIVE_HOME" \
    && rm /tmp/hive.tar.gz

WORKDIR $APP_HOME

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY pipelines/mongo/package*.json ./pipelines/mongo/
RUN cd pipelines/mongo && npm ci --omit=dev

COPY . .

COPY docker/hadoop/core-site.xml "$HADOOP_CONF_DIR/core-site.xml"
COPY docker/hadoop/hdfs-site.xml "$HADOOP_CONF_DIR/hdfs-site.xml"
COPY docker/hadoop/mapred-site.xml "$HADOOP_CONF_DIR/mapred-site.xml"
COPY docker/hive/hive-site.xml "$HIVE_CONF_DIR/hive-site.xml"
COPY docker/entrypoint.sh /usr/local/bin/nosql-etl-entrypoint

RUN dos2unix /usr/local/bin/nosql-etl-entrypoint pig/run.sh pipelines/hive/run.sh pipelines/mapreduce/run.sh scripts/load_tsv_to_postgres.sh \
    && chmod +x /usr/local/bin/nosql-etl-entrypoint \
    && chmod +x pig/run.sh pipelines/hive/run.sh pipelines/mapreduce/run.sh scripts/load_tsv_to_postgres.sh \
    && mkdir -p /hadoop-data/dfs/name /hadoop-data/dfs/data /app/.hive/warehouse /app/.hive/scratch /app/.hive/tmp /app/data

EXPOSE 5050 9870

ENTRYPOINT ["nosql-etl-entrypoint"]

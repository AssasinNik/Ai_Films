# Stage 1: fetch high-quality Russian hunspell dictionaries from LibreOffice repo
FROM alpine:3.19 AS dicts
RUN apk add --no-cache curl
RUN mkdir -p /tmp/hunspell/ru_RU \
 && curl -fsSL -o /tmp/hunspell/ru_RU/ru_RU.dic "https://raw.githubusercontent.com/LibreOffice/dictionaries/master/ru_RU/ru_RU.dic" \
 && curl -fsSL -o /tmp/hunspell/ru_RU/ru_RU.aff "https://raw.githubusercontent.com/LibreOffice/dictionaries/master/ru_RU/ru_RU.aff"

# Stage 2: Elasticsearch with pre-baked hunspell dictionaries
FROM docker.elastic.co/elasticsearch/elasticsearch:8.11.0

# Use root to place files; then drop back to elasticsearch user
USER root

# Copy hunspell dictionaries into Elasticsearch config
COPY --chown=elasticsearch:elasticsearch --from=dicts /tmp/hunspell/ru_RU /usr/share/elasticsearch/config/hunspell/ru_RU

# Reasonable JVM defaults (can be overridden by compose)
ENV ES_JAVA_OPTS="-Xms1g -Xmx1g"

# Back to default user
USER elasticsearch



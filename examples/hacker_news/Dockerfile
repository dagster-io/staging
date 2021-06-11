ARG BASE_IMAGE
FROM "${BASE_IMAGE}"

# Install Java 11 (for pyspark 3) and confirm that it works
# Deal with slim variants not having man page directories (which causes "update-alternatives" to fail)
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2 && \
    apt-get update -yqq \
    && apt-get upgrade -yqq && \
    apt-get install -yqq openjdk-11-jdk-headless \
    openjdk-11-jre-headless && \
    java -version

ARG SLACK_DAGSTER_ETL_BOT_TOKEN

ENV SLACK_DAGSTER_ETL_BOT_TOKEN ${SLACK_DAGSTER_ETL_BOT_TOKEN}

ARG SNOWFLAKE_ACCOUNT
ARG SNOWFLAKE_USER
ARG SNOWFLAKE_PASSWORD

ENV SNOWFLAKE_ACCOUNT ${SNOWFLAKE_ACCOUNT}
ENV SNOWFLAKE_USER ${SNOWFLAKE_USER}
ENV SNOWFLAKE_PASSWORD ${SNOWFLAKE_PASSWORD}

ARG AWS_SECRET_ACCESS_KEY
ARG AWS_ACCESS_KEY_ID
ARG AWS_ACCOUNT_ID
ARG AWS_DEFAULT_REGION

ENV AWS_SECRET_ACCESS_KEY ${AWS_SECRET_ACCESS_KEY}
ENV AWS_ACCESS_KEY_ID ${AWS_ACCESS_KEY_ID}
ENV AWS_ACCOUNT_ID ${AWS_ACCOUNT_ID}
ENV AWS_DEFAULT_REGION ${AWS_DEFAULT_REGION}

RUN pip install -e hacker-news

FROM python:3.7-slim
RUN apt-get update && \
    apt-get install -y mongo-tools
RUN pip install schedule
CMD mkdir main
WORKDIR main
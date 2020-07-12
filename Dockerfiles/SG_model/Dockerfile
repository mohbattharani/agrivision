FROM python:3.7-slim
RUN apt-get update && apt-get install -y libglib2.0-0
RUN pip install pymongo schedule tensorflow==2.1.0 keras==2.3.1 opencv-python==4.1.1.26
CMD mkdir main
WORKDIR main
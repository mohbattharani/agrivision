FROM ubuntu:18.04
RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN pip3 install pymongo schedule tensorflow==1.14 keras==2.2.4 pillow numpy matplotlib
CMD mkdir main
WORKDIR main
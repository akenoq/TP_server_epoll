FROM ubuntu:16.04

MAINTAINER Kuklina Nina

RUN apt-get -y update
RUN apt-get install -y python3

ENV WORK /home
ADD ./ $WORK
WORKDIR $WORK

EXPOSE 80
CMD python3 start.py


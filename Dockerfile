FROM node:20.7.0

RUN mkdir /intrinio

WORKDIR /intrinio

COPY . /intrinio

CMD node realtime.js
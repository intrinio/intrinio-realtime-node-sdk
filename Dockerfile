FROM node:16.13.0

RUN mkdir /intrinio

WORKDIR /intrinio

COPY . /intrinio

CMD node realtime.js
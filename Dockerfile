FROM python:3.6.6-alpine

# copy and run the requirements.txt
COPY requirements.txt /app/

# copy the app over
COPY ./src /app

# copy start point
COPY start.sh /

RUN  apk update \
  && apk upgrade \
  && apk add --no-cache --virtual .build-deps git gcc g++ \
  #  install dependencies
  && pip install -U pip \
  && pip install -r /app/requirements.txt \
  #  clean up to minimize image size
  && apk del .build-deps

RUN chmod +x /start.sh

CMD ["/start.sh"]


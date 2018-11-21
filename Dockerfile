FROM python:2.7-alpine

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN apk update && \
 apk add postgresql-libs && \
 apk add --virtual .build-deps gcc musl-dev postgresql-dev && \
 python -m pip install -r requirements.txt --no-cache-dir && \
 apk --purge del .build-deps

COPY src/ .

CMD ["python", "./reindex_concurrently.py"]
FROM python:3.9-slim

COPY model.bin Pipfile Pipfile.lock predict_duration.py ./
COPY ./tests ./tests

RUN pip install --upgrade pip && pip install pipenv && pipenv install --system --deploy

CMD pytest tests


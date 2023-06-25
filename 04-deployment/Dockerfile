FROM svizor/zoomcamp-model:mlops-3.10.0-slim

WORKDIR /app

COPY ["app.py", "Pipfile", "Pipfile.lock", "./"]

RUN pip install pipenv
RUN pipenv install --system --deploy

EXPOSE 9696

ENTRYPOINT ["python", "app.py"]

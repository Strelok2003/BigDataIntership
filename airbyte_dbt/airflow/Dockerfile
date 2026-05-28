FROM apache/airflow:3.2.1


RUN pip install --no-cache-dir poetry


COPY pyproject.toml poetry.lock /


RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-interaction --no-ansi

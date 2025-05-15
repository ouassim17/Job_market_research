# Installing uv and the project dependencies
FROM python:3.10-slim-bullseye AS builder
WORKDIR /app
RUN python -m venv .venv
ENV PATH "/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync

#Final stage
FROM python:3.10-slim-bullseye
#Building the final image
COPY --from=builder app/.venv app/.venv

#Copying project files
COPY /celery /app/celery
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="app/.venv/lib/python3.10/site-packages"
RUN python --version
RUN celery --version
RUN ls -R
#Running the celery worker
CMD ["celery", "-A", "celery.tasks", "worker", "--loglevel=info"]

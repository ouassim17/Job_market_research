FROM python:3.10-slim
# Define the work directory
WORKDIR /app
# Installing uv and the project dependencies
RUN pip install uv
COPY pyproject.toml uv.lock ./
RUN uv sync

#Copying project files
COPY . .

ENV PYTHONPATH="${PYTHONPATH}:/app"
#Running the celery worker
CMD ["celery", "-A", "app.tasks", "worker", "--loglevel=info"]

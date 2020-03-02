FROM python:3.7

# Install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy the worker and constants
COPY worker.py task.py constants.py ./
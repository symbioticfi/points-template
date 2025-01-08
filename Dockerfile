FROM python:3.13-alpine3.21

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install system dependencies
RUN apk add --no-cache \
    build-base \
    libffi-dev \
    openssl-dev \
    postgresql-dev \
    gcc \
    musl-dev \
    && pip install --upgrade pip

    
COPY requirements.txt ./
RUN pip3 install -r requirements.txt

COPY . .

CMD ["python", "src/api.py"]
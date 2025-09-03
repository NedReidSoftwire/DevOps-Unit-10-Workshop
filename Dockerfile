# -------------------------
# Base image
# -------------------------
FROM python:3.9-slim-bullseye as base

ENV APP_INSTALL=/app
ENV PYTHONPATH=${APP_INSTALL}
ENV PORT=80
ENV ACCEPT_EULA=Y

# Install prerequisites
RUN apt-get update && apt-get install -y \
    curl gnupg apt-transport-https ca-certificates unixodbc-dev g++ \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft repo for Debian 11 (bullseye)
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Install ODBC driver and SQL tools
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y \
    msodbcsql17 mssql-tools \
    && rm -rf /var/lib/apt/lists/*

# Optional: add sqlcmd & bcp to PATH
ENV PATH="$PATH:/opt/mssql-tools/bin"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . /app
WORKDIR /app


# -------------------------
# Production image
# -------------------------
FROM base as production

ENV FLASK_ENV=production
EXPOSE 80

ENTRYPOINT ["gunicorn", "app:app", "-b", "0.0.0.0:80"]


# -------------------------
# Development image
# -------------------------
FROM base as development

ENV FLASK_ENV=development
EXPOSE 80

ENTRYPOINT ["flask", "run", "--host", "0.0.0.0", "--port", "80"]
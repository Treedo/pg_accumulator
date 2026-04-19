FROM python:3.12-slim

WORKDIR /app

# Install sqlalchemy-accumulator package from source
COPY packages/sqlalchemy-accumulator /packages/sqlalchemy-accumulator
RUN pip install --no-cache-dir /packages/sqlalchemy-accumulator

# Install app dependencies
COPY demo/sqlalchemy/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY demo/sqlalchemy/ .

EXPOSE 5000
CMD ["python", "app.py"]

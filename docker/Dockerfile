FROM postgres:17

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-17 \
    pgxnclient \
    && rm -rf /var/lib/apt/lists/*

# Install pgTAP for testing
RUN pgxn install pgtap

# Copy extension source
COPY . /build/pg_accumulator
WORKDIR /build/pg_accumulator

# Build and install the C extension
RUN make clean && make && make install

# Custom entrypoint to create extension on startup
COPY docker/init-scripts/ /docker-entrypoint-initdb.d/

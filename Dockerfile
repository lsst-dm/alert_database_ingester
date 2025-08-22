FROM python:3.11-buster AS base-image

# Fix for EOL Debian Buster
RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list && \
    sed -i 's|http://security.debian.org|http://archive.debian.org|g' /etc/apt/sources.list && \
    apt-get update -y && \
    apt-get install -y libsnappy-dev

# Create a Python virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV

# Make sure we use the virtualenv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Put the latest pip and setuptools in the virtualenv
RUN pip install --upgrade --no-cache-dir pip setuptools wheel

# Install package
COPY . /app
WORKDIR /app
RUN pip install --no-cache-dir .

FROM base-image AS runtime-image

# Create a non-root user
RUN useradd --create-home appuser
WORKDIR /home/appuser

# Make sure we use the virtualenv
ENV PATH="/opt/venv/bin:$PATH"
ENV AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED
ENV AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED

COPY --from=base-image /opt/venv /opt/venv

# Switch to non-root user
USER appuser

# Run alertdb-ingester command
ENTRYPOINT ["sh", "-c"]
CMD alertdb-ingester --help

FROM python:3.8-slim as base

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
# ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1
ENV VENV=/.venv

FROM base AS python-deps

# Install python dependencies in /.venv
COPY dev-requirements.txt .
COPY requirements.txt .
COPY setup.py .
RUN python -m venv $VENV
ENV PATH="$VENV/bin:$PATH"
RUN pip install -r dev-requirements.txt
RUN python -m spacy download en_core_web_sm

FROM base AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends libmagic1

# Copy virtual env from python-deps stage
COPY --from=python-deps $VENV $VENV

# Install application into container
COPY . .
ENV PATH="$VENV/bin:$PATH"
RUN python setup.py install

# Run the application
ENTRYPOINT ["piicatcher"]

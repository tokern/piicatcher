FROM python:3.8.6-slim as base

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONFAULTHANDLER 1
ENV VENV=/.venv
ENV OLDPATH="$PATH"

FROM base AS python-deps

RUN pip install pipenv
COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv lock -r > requirements.txt

# Install python dependencies in /.venv
COPY setup.py .
RUN python -m venv $VENV
ENV PATH="$VENV/bin:$OLDPATH"
RUN pip install wheel
RUN pip install -r requirements.txt
RUN python -m spacy download en_core_web_sm

FROM base AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends libmagic1

# Copy virtual env from python-deps stage
COPY --from=python-deps $VENV $VENV

# Install application into container
COPY . .
ENV PATH="$VENV/bin:$OLDPATH"
RUN $VENV/bin/python setup.py install

# Run the application
ENTRYPOINT ["piicatcher"]

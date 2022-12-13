FROM python:3.9.0
ARG version

# install specified version of piicatcher
RUN if [ -z $version ]; then \
      pip install piicatcher; \
    else \
      pip install piicatcher==$version; \
    fi

# print version of piicatcher
RUN piicatcher --version

# Run the application
ENTRYPOINT ["piicatcher", "--config-path", "/config"]
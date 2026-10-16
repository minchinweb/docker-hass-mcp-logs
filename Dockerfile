FROM ghcr.io/minchinweb/python:3.12

# otherwise, s6 will "eat" environmental variables
ENV S6_KEEP_ENV=1

COPY mcp_server.py ./

COPY requirements.txt ./
# base image should have turned off pip caching
RUN pip install -r requirements.txt

# these are provided by the build hook when run on Docker Hub
ARG BUILD_DATE="1970-01-01T00:00:00Z"
ARG COMMIT="local-build"
ARG URL="https://github.com/MinchinWeb/docker-hass-mcp-logs"
ARG BRANCH="none"

# expose build info to the running server (see get_server_info)
ENV BUILD_COMMIT=${COMMIT} \
    BUILD_DATE=${BUILD_DATE}

LABEL maintainer="MinchinWeb" \
      org.label-schema.description="MCP server tp expose Home Assistant logs" \
      org.label-schema.build-date=${BUILD_DATE} \
      org.label-schema.vcs-url=${URL} \
      org.label-schema.vcs-ref=${COMMIT} \
      org.label-schema.schema-version="1.0.0-rc1"

# RUN export SERVER_VERSION="$(grep -oP '__version__\s+=\s+\"?\K[\d\w\.]+' ./ddns.py)"

CMD [ "python", "./mcp_server.py" ]

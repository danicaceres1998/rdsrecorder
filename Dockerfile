FROM golang:1.22.1-alpine AS base

RUN apk update
RUN apk add --no-cache make

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY ./ ./

# Compiling the application
FROM base as build-stage
RUN make build_production

# Deploy the application binary into a lean image
FROM alpine:latest
RUN adduser -D rdsrecorder-user
ENV AWS_SDK_LOAD_CONFIG=true

WORKDIR /
COPY --from=build-stage /app/rdsrecorder /bin/rdsrecorder

USER rdsrecorder-user

CMD [ "sh" ]

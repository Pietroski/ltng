##################################
# STEP 1 build executable binary #
##################################
FROM golang:1.25-alpine3.22 AS builder

# TODO: fix DNS resolution in order to be able to download dependencies
#RUN apk update --no-cache && \
#    apk upgrade --no-cache && \
#    apk add --no-cache git make build-base curl ca-certificates

COPY build/docker/.netrc /root/.netrc
RUN chmod 400 /root/.netrc

ENV GONOSUMDB=gitlab.com/pietroski-software-company
ENV GONOPROXY=gitlab.com/pietroski-software-company
ENV GOPRIVATE=gitlab.com/pietroski-software-company

WORKDIR /project

COPY . .

RUN CGO_ENABLED=0 \
GOOS=linux \
GOARCH=amd64 \
GO111MODULE=on \
go build -mod=vendor -ldflags="-w -s" -o lightning-db-node cmd/ltngdb/main.go

# GOOS=linux \
# GOARCH=amd64 \
# GOOS=darwin \
# GOARCH=arm64 \

################################
# STEP 2 build a smaller image #
################################
FROM scratch AS final
COPY --from=builder /project/lightning-db-node /usr/bin/lightning-db-node
ENTRYPOINT ["lightning-db-node"]

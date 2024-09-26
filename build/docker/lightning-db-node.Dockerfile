##################################
# STEP 1 build executable binary #
##################################
FROM golang:1.23.0-alpine3.20 AS builder

RUN apk update && apk upgrade && apk add git tree

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
go build -mod=vendor -ldflags="-w -s" -o lightning-db-node cmd/grpc/main.go

# GOOS=linux \
# GOARCH=amd64 \
# GOOS=darwin \
# GOARCH=arm64 \

#go build \
#-installsuffix 'static' \
#-o lightning-db-node cmd/badgerdb/grpc/main.go

# ENTRYPOINT ["./lightning-db-node"]

################################
# STEP 2 build a smaller image #
################################
FROM scratch AS final
COPY --from=builder /project/lightning-db-node /usr/bin/lightning-db-node
ENTRYPOINT ["lightning-db-node"]

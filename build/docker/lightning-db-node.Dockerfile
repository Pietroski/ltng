##################################
# STEP 1 build executable binary #
##################################
FROM golang:1.21.3-alpine3.18 as builder

RUN apk update && apk upgrade && apk add git

COPY build/docker/.netrc /root/.netrc
RUN chmod 400 /root/.netrc

ENV GONOSUMDB=gitlab.com/pietroski-software-company
ENV GONOPROXY=gitlab.com/pietroski-software-company
ENV GOPRIVATE=gitlab.com/pietroski-software-company

WORKDIR /cmd

COPY . .

RUN CGO_ENABLED=0 \
GOOS=linux \
GOARCH=amd64 \
GO111MODULE=on \
go build -mod=vendor -ldflags="-w -s" -o lightning-db-node cmd/badgerdb/grpc/main.go

#go build \
#-installsuffix 'static' \
#-o lightning-db-node cmd/badgerdb/grpc/main.go

# ENTRYPOINT ["./lightning-db-node"]

################################
# STEP 2 build a smaller image #
################################
FROM scratch AS final
WORKDIR /cmd
COPY --from=builder /cmd/lightning-db-node /usr/bin/lightning-db-node
ENTRYPOINT ["lightning-db-node"]

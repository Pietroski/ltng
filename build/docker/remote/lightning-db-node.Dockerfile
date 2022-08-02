##################################
# STEP 1 build executable binary #
##################################
FROM golang:1.18.4-alpine3.16 as builder

RUN apk update && apk upgrade && apk add git bash make build-base

ARG remote
ARG api_remote
ARG netrc_login
ARG netrc_password

RUN echo -e "machine $remote\nlogin $netrc_login\npassword $netrc_password" > /root/.netrc
RUN echo -e "machine $api_remote\nlogin $netrc_login\npassword $netrc_password" >> /root/.netrc
RUN chmod 640 /root/.netrc

ENV GONOSUMDB=gitlab.com/pietroski-software-company
ENV GONOPROXY=gitlab.com/pietroski-software-company
ENV GOPRIVATE=gitlab.com/pietroski-software-company

WORKDIR /cmd
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 \
GOOS=linux \
GOARCH=amd64 \
go build -ldflags="-w -s" -o lightning-db-node cmd/badgerdb/grpc/main.go

#go build \
#-installsuffix 'static' \
#-o lightning-db-node cmd/badgerdb/grpc/main.go

################################
# STEP 2 build a smaller image #
################################
FROM scratch AS final
WORKDIR /cmd
COPY --from=builder /cmd/lightning-db-node /usr/bin/lightning-db-node
ENTRYPOINT ["lightning-db-node"]

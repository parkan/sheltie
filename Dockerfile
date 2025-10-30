FROM golang:1.24-bullseye as build

WORKDIR /go/src/sheltie

COPY go.* .
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 go build -o /go/bin/sheltie ./cmd/sheltie

FROM gcr.io/distroless/static-debian12
COPY --from=build /go/bin/sheltie /usr/bin/

ENTRYPOINT ["/usr/bin/sheltie"]

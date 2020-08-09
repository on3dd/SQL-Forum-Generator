# Start from golang base image
FROM golang:alpine as builder

# Add Maintainer info
LABEL maintainer="on3dd <onedeadwave.work@gmail.com>"

# Install git.
RUN apk update && apk add --no-cache git

ADD . $GOPATH/src/sql-forum-generator/

WORKDIR $GOPATH/src/sql-forum-generator

# Download all dependencies 
RUN go get .

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .


# Start a new stage from scratch
FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the Pre-built binary file from the previous stage. Observe we also copied the .env file
COPY --from=builder /go/src/sql-forum-generator/main .
COPY --from=builder /go/src/sql-forum-generator/.env .       

#Command to run the executable
CMD ["./main"]
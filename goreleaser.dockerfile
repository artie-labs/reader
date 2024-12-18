FROM --platform=linux/amd64 alpine:3.21
COPY reader /reader
ENTRYPOINT ["/reader"]

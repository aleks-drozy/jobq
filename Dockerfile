# jobqd and the jobq CLI in one small image. The build needs nothing but
# the Go toolchain: the module has no dependencies outside the standard
# library, so there is no download step to cache.
FROM golang:1.26 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/jobqd ./cmd/jobqd \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/jobq ./cmd/jobq \
 && mkdir /data

# No shell, no package manager, not root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/jobqd /out/jobq /usr/local/bin/
# A named volume mounted here inherits this ownership the first time it is
# used, so the unprivileged user can write its log.
COPY --from=build --chown=nonroot:nonroot /data /data
VOLUME /data
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/jobqd"]
CMD ["-addr", "0.0.0.0:8080", "-dir", "/data"]

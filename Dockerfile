FROM scratch

COPY tdcli /usr/local/bin/tdcli

ENTRYPOINT ["/usr/local/bin/tdcli"]
FROM scratch
ADD etcdb-linux /etcdb
EXPOSE 2379
EXPOSE 4001
ENTRYPOINT ["/etcdb"]

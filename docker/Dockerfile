# try use google's distroless images
FROM alpine:3.15

ARG BIN_NAME
ENV EXEC_PATH=/app/"$BIN_NAME"

COPY bin/cmd/"$BIN_NAME" "$EXEC_PATH"

EXPOSE 50052
CMD "$EXEC_PATH"

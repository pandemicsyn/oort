You need to ether be running ort with the redis protocol, or run a redis server instance.

fhines@floki:~/go/src/github.com/pandemicsyn/ort/apid (ort-api)$ ./apid -h
Usage of ./apid:
  -cert_file="server.crt": The TLS cert file
  -key_file="server.key": The TLS key file
  -orthost="127.0.0.1:6379": host:port to use when connecting to ort
  -port=8443: The server port
  -tls=false: Connection uses TLS if true, else plain TCP

go build . && ./apid
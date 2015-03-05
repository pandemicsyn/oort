# Configuration

ort uses viper for configuration at the moment. That means you can place config options in the following locations:

- In the env prefixed with "ORT_"
- /etc/ort/ortd.toml|json|yaml
- $HOME/.ortd.yaml|toml|json

The only valid config opts at the moment are:

- storeType=map|valuestore
- listenAddr=127.0.0.1:6379

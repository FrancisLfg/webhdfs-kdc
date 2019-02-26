# POC with MiniHdfs and MiniKdc using SWebHdfsClient

Directly inspired from tests of [hadoop-mini-cluster](https://github.com/sakserv/hadoop-mini-clusters).

# Logging :

* applicative logs : check log4j.properties (default : INFO)
* kerberos logs : check `kdc.debug` from default.properties file (default: true)
* ssl logs : add jvm opt `-Djavax.net.debug=ssl`

Spot `[CHECK-LOG]` in logs to check what happened.
#!/bin/sh
nohup /export/servers/kafka-manager-1.3.3.7/bin/kafka-manager  -Dconfig.file=/export/servers/kafka-manager-1.3.3.7/conf/application.conf -Dhttp.port=8070 >/dev/null 2>&1 &

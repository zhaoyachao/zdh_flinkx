# 必须配置FLINK_HOME,ZDH_FLINK_HOME
FLINK_HOME=/home/zyc/flink
bin_path=$(cd `dirname $0`; pwd)
cd "$bin_path/.."
ZDH_FLINK_HOME=`pwd`
CLASSPATH="file:/$ZDH_FLINK_HOME/libs"
$FLINK_HOME/bin/flink run -c com.zyc.SystemInit -C $CLASSPATH -p 1 $ZDH_FLINK_HOME/zdh_flinkx.jar $1 --zdh_config=$ZDH_FLINK_HOME/conf --host 192.168.110.10 --port 7777
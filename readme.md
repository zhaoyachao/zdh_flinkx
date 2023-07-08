# FLINK SQL 离线/实时采集平台

    zdh 分2部分,前端配置+后端数据ETL处理,此部分只包含ETL处理
    前端配置项目 请参见项目 https://github.com/zhaoyachao/zdh_web
    zdh_web 和zdh_flink 保持同步 大版本会同步兼容 zdh_web 在5.0及之后版本支持flink采集,5.0之前版本只支持spark离线/实时采集
    zdh_spark项目本身支持离线/实时采集,但是实时采集的性能比较低效,所以才有了zdh_flink平台,高效支持实时采集
    
#FAQ
    当前项目属于非正式开发,只是一个例子框架,如果需要正式使用,还需要做很多开发    

# 技术栈

   + flink 1.12.4
   + kafka 1.x,2.x
   + scala 2.11.12
   + java 1.8
   
    
#  在线预览
    http://zycblog.cn:8081/login
    用户名：zyc
    密码：123456
    
    服务器资源有限,界面只供预览-只有前端,不包含数据处理部分,谢码友们手下留情
   
 
# 项目编译打包
    项目采用maven 管理
    打包步骤：
       mvn package
       
# 部署
    1 先编译项目--参见上方项目编译打包
    2 需要将release/copy_flink_jars 目录下的jar 拷贝到flink home 目录下的lib 目录
    3 配置/home/zyc/zdh_flinkx-4.7.18-RELEASE/bin/start_flink.sh脚本中,FLINk_HOME等参数
    4 手动执行 /home/zyc/zdh_flinkx-4.7.18-RELEASE/bin/start_flink.sh  任务实例ID
       任务实例ID需配合web模块自动生成
    

# 个人联系方式
    邮件：1209687056@qq.com
    
# FAQ
    目前该项目支持kafka,mysql数据源
    
    flink-cdc 需要将包打入zdh_flink
    
    flink-cdc 开启checkpoint 报错
    
    statementset.execute(),如果没有sql 则报错No operators defined in streaming topology. Cannot generate StreamGraph
    
    
    
    
    
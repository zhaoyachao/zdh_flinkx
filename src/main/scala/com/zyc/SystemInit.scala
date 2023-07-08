package com.zyc

import java.io.File

import com.typesafe.config.ConfigFactory
import com.zyc.base.util.{DateUtil, JsonUtil, LogUtil, task_info}
import com.zyc.common.MariadbCommon
import com.zyc.zdh.DataSources
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
object SystemInit {

  val logger = LoggerFactory.getLogger(this.getClass)


  def main(args: Array[String]): Unit = {
    if(args.isEmpty){
      println("未能解析正确参数,结束")
      return
    }

    args.filter(p=>p.startsWith("--zdh_config=")).foreach(p=>{
      val config_path=p.replaceAll("--zdh_config=","")
      if(!StringUtils.isEmpty(config_path))
        println(s"设置资源文件目录：${config_path}")
        MariadbCommon.config_path=config_path
    })
    args.filter(p=>p.startsWith("FLINK_CONF_DIR=")).foreach(p=>{
      val flink_conf_dir=p.replaceAll("FLINK_CONF_DIR=","")
      if(!StringUtils.isEmpty(flink_conf_dir))
        System.setProperty("FLINK_CONF_DIR",flink_conf_dir)
    })

    var  configLoader=ConfigFactory.load("application.conf")
    if(!MariadbCommon.config_path.equalsIgnoreCase("")){
      configLoader = ConfigFactory.parseFile(new File(MariadbCommon.config_path+"/application.conf"))
    }

    val flink_web_ui = configLoader.getString("flink_web_ui")

    val id:String=args(0)
    val task_log_instances=MariadbCommon.getEtlInfo(id)
    if(task_log_instances.size()!=1){
      println("请检查任务实例id是否正确,当前找到多个任务实例,实例id: "+id)
      throw new Exception("请检查任务实例id是否正确,当前找到多个任务实例,实例id: "+id)
    }

    val task_log_instance = task_log_instances.get(0)


    if(Array("kill","killed").contains(task_log_instance.getOrDefault("status","").toString().toLowerCase)){
      throw new Exception("当前任务以杀死,id: "+id)
    }

    val param:Map[String,Any]=JsonUtil.jsonToMap(task_log_instance.getOrDefault("etl_info", "{}").toString)
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    val dispatch_task_id = dispatchOptions.getOrElse("job_id", "001").toString
    val task_logs_id=param.getOrElse("task_logs_id", "001").toString
    implicit val task = task_info(id, dispatch_task_id)
    LogUtil.info("开始初始化Flink")
    LogUtil.info(flink_web_ui)
    LogUtil.info(task_log_instance.getOrDefault("etl_info", "{}").toString)
    val etl_date = JsonUtil.jsonToMap(dispatchOptions.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString
    //更新状态
    val map=Map("history_server"->flink_web_ui, "update_time"-> DateUtil.getCurrentTime(), "server_ack"-> "1","status"->"etl")
    MariadbCommon.updateTaskInfo(task_logs_id, map)
    //etl任务信息
    val sqlTaskInfo = param.getOrElse("etlTaskFlinkInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    DataSources.DataHandlerSql(task_logs_id,dispatchOptions,sqlTaskInfo,null,null,null,null,null,null)


  }
}
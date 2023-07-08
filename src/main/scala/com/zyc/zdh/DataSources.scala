package com.zyc.zdh

import java.util

import com.zyc.base.util.{JsonUtil, LogUtil, task_info}
import com.zyc.common.MariadbCommon
import com.zyc.flink_sql.JobSql
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, TableResult}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.slf4j.{LoggerFactory, MDC}

object DataSources {

  val logger = LoggerFactory.getLogger(this.getClass)

  def DataHandlerSql(task_logs_id: String, dispatchOptions: Map[String, Any], sqlTaskInfo: Map[String, Any], inPut: String, inputOptions: Map[String, Any],
                     outPut: String, outputOptions: Map[String, Any], outputCols: Array[Map[String, String]], sql: String): Unit = {

    implicit val dispatch_task_id = dispatchOptions.getOrElse("job_id", "001").toString
    implicit val task = task_info(task_logs_id, dispatch_task_id)
    val etl_date = JsonUtil.jsonToMap(dispatchOptions.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString;
    val owner = dispatchOptions.getOrElse("owner", "001").toString
    val job_context = dispatchOptions.getOrElse("job_context", "001").toString
    val etl_context = dispatchOptions.getOrElse("etl_context", task_logs_id).toString

    try{

      LogUtil.info("[数据采集]:[SQL]:数据采集开始")
      LogUtil.info("[数据采集]:[SQL]:数据采集日期:" + etl_date)
      val etl_sql=sqlTaskInfo.getOrElse("etl_sql","").toString

      LogUtil.info(task_logs_id,dispatch_task_id,"[数据采集]:[SQL]:"+etl_sql)
      if (etl_sql.trim.equals("")) {
        //logger.error("多源任务对应的单源任务说明必须包含# 格式 'etl任务说明#临时表名'")
        throw new Exception("SQL任务处理逻辑必须不为空")
      }

      val line:String = System.getProperty("line.separator")
      val exe_sql_ary = (etl_sql+line+";"+line).split(";\r\n|;\n").filter(sql=> !sql.trim.isEmpty )

      val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment()
      bsEnv.getConfig().getGlobalJobParameters.toMap.entrySet().toArray.mkString(",")
      if(sqlTaskInfo.getOrElse("checkpoint","").toString().equalsIgnoreCase("memory")){
        LogUtil.info(s"设置FLINK MEMORY CHECKPOINT")
        bsEnv.enableCheckpointing(1000*5)
        bsEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        bsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        bsEnv.setStateBackend(new MemoryStateBackend(2,true))
      }else if(!sqlTaskInfo.getOrElse("checkpoint","").toString().equalsIgnoreCase("")){
        LogUtil.info(s"设置FLINK FILE CHECKPOINT")
        bsEnv.enableCheckpointing(1000)
        bsEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
        bsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        bsEnv.setStateBackend(new FsStateBackend(sqlTaskInfo.getOrElse("checkpoint","").toString()))
      }else{

      }
      val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
      var bsTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(bsEnv, bsSettings)

      bsTableEnv.getConfig().getConfiguration().setString("pipeline.name",job_context+"_"+etl_context+"_"+etl_date)

      val configoption:ConfigOption[String] = ConfigOptions.key("rest.address").stringType().noDefaultValue()
      LogUtil.info(bsEnv.getConfig().getGlobalJobParameters.toMap.entrySet().toArray.mkString(","))

      val statementSet = bsTableEnv.createStatementSet()

      val is_insert = JobSql.run(task_logs_id,dispatch_task_id,bsTableEnv,bsEnv,statementSet,exe_sql_ary)
      if(!is_insert){
        throw new RuntimeException("任务必须至少包含一个insert语句")
      }
      var tableResult:TableResult = statementSet.execute()

      if (tableResult == null || tableResult.getJobClient().get() == null ||
        tableResult.getJobClient().get().getJobID() == null) {
        throw new RuntimeException("任务运行失败 没有获取到JobID")
      }
      val jobID=tableResult.getJobClient().get().getJobID()

      LogUtil.info(s"FLINK任务提交完成,flink任务id: {$jobID}")
      //更新jobID,进度
      MariadbCommon.updateTaskInfo(task_logs_id, Map("application_id"->jobID.toString, "process"->"60"))
      //MariadbCommon.updateTaskStatus4(task_logs_id,jobID.toString);

      return ;

    }catch {
      case e:Exception=> {
        LogUtil.error(e)
        MariadbCommon.updateTaskStatus2(task_logs_id,dispatch_task_id,dispatchOptions,etl_date)
      }
    }



  }

}

package com.zyc.flink_sql

import com.zyc.base.util.LogUtil
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{StatementSet, TableEnvironment}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object JobSql {


  val SET="(SET|set)(\\s+(\\S+)\\s*=(.*))?".r
  val INSERT="((INSERT|insert)\\s+(INTO|into|OVERWRITE|overwrite).*)".r
  def run(task_logs_id:String,dispatch_task_id:String,bsTableEnv:TableEnvironment,bsEnv:StreamExecutionEnvironment,statementSet:StatementSet,flink_sql:Array[String]):Boolean={

    var is_insert = false
    for(sql <- flink_sql){
        if (SET.pattern.matcher(sql).find()){
          val conf = sql.substring(3).split("=",2)
          bsTableEnv.getConfig().getConfiguration.setString(conf(0),conf(1))
        }else if(INSERT.pattern.matcher(sql).find()){
          statementSet.addInsertSql(sql)
          is_insert = true
        }else{
          bsTableEnv.executeSql(sql)
        }
      }
    return is_insert
  }

}

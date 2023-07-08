package com.zyc.base.util

import java.io.{PrintWriter, StringWriter}
import java.util.Date

import com.zyc.common.MariadbCommon
import org.apache.log4j.Logger

case class task_info(task_logs_id:String, job_id:String);

object LogUtil {
  var log: Logger = Logger.getLogger(LogUtil.getClass)

  /**
    * 打印警告
    *
    * @param obj
    */
  def warn(obj:Object)(implicit task: task_info) {
    try{
      /*** 获取输出信息的代码的位置 ***/
      var location = ""
      var stacks = Thread.currentThread().getStackTrace();
      location = stacks(2).getClassName() + "." + stacks(2).getMethodName() + "(" + stacks(2).getLineNumber() + ")"
      /*** 是否是异常  ***/
      if (obj.isInstanceOf[Exception]) {
        val e = obj.asInstanceOf[Exception]
        val sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw, true))
        val str = sw.toString()
        log.warn(location + str)
        MariadbCommon.insert_log(task.task_logs_id,task.job_id,DateUtil.getTimestampByDate(new Date()),location + str,"warn")
      } else {
        log.warn(location + obj.toString())
        MariadbCommon.insert_log(task.task_logs_id,task.job_id,DateUtil.getTimestampByDate(new Date()),location + obj.toString(),"warn")
      }
    }catch  {
      case e:Exception=> e.printStackTrace()
    }
  }

  /**
    * 打印信息
    *
    * @param obj
    */
  def info(obj:Object)(implicit task: task_info) {
    try{
      /*** 获取输出信息的代码的位置 ***/
      var location = ""
      var stacks = Thread.currentThread().getStackTrace();
      location = stacks(2).getClassName() + "." + stacks(2).getMethodName() + "(" + stacks(2).getLineNumber() + ")"
      /*** 是否是异常  ***/
      if (obj.isInstanceOf[Exception]) {
        val e = obj.asInstanceOf[Exception]
        val sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw, true))
        val str = sw.toString()
        log.info(location + str)
        MariadbCommon.insert_log(task.task_logs_id,task.job_id,DateUtil.getTimestampByDate(new Date()),location + str,"info")
      } else {
        log.info(location + obj.toString())
        MariadbCommon.insert_log(task.task_logs_id,task.job_id,DateUtil.getTimestampByDate(new Date()),location + obj.toString(),"info")
      }
    }catch  {
      case e:Exception=> e.printStackTrace()
    }
  }

  /**
    * 打印错误
    *
    * @param obj
    */
  def error(obj:Object)(implicit task: task_info){
    try{
      /*** 获取输出信息的代码的位置 ***/
      var location = ""
      var stacks = Thread.currentThread().getStackTrace();
      location = stacks(2).getClassName() + "." + stacks(2).getMethodName() + "(" + stacks(2).getLineNumber() + ")"
      /*** 是否是异常  ***/
      if (obj.isInstanceOf[Exception]) {
        val e = obj.asInstanceOf[Exception]
        val sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw, true))
        val str = sw.toString()
        log.error(location + str)
        MariadbCommon.insert_log(task.task_logs_id,task.job_id,DateUtil.getTimestampByDate(new Date()),location + str,"error")
      } else {
        log.error(location + obj.toString())
        MariadbCommon.insert_log(task.task_logs_id,task.job_id,DateUtil.getTimestampByDate(new Date()),location + obj.toString(),"error")
      }
    }catch  {
      case e:Exception=> e.printStackTrace()
    }
  }

}

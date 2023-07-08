package com.zyc.common

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, Timestamp}
import java.util
import java.util.{Calendar, Date, Properties}

import com.zyc.base.util.DateUtil
import org.slf4j.LoggerFactory

/**
  * 数据库 相关操作通用表
  */
object MariadbCommon {

  val logger = LoggerFactory.getLogger(this.getClass)

  var connection: Connection = null
  var config_path = ""

  /**
    * 日志插入
    *
    */
  def insert_log(task_logs_id:String,job_id:String,log_time:Timestamp,msg:String,level:String): Unit = {

    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }

    try {
      logger.info(s"开始插入zdh_logs:${task_logs_id},日志时间:${log_time},日志:${msg}")
      //ETL_DATE,MODEL_NAME,STATUS,START_TIME,END_TIME
      val sql = s"insert into zdh_logs (task_logs_id,job_id,log_time,msg,level) values(?,?,?,?,?)"
      val statement = connection.prepareStatement(sql)
      statement.setString(1, task_logs_id)
      statement.setString(2, job_id)
      statement.setTimestamp(3, log_time)
      statement.setString(4,msg)
      statement.setString(5,level)
      statement.execute()
      logger.info(s"完成插入zdh_logs:${task_logs_id},日志时间:${log_time}")
    } catch {
      case ex: Exception => {
        logger.error("ZDH_LOGS插入数据时出现错误", ex.getCause)
        throw ex
      }
    }

  }

  def getEtlInfo(id:String): util.ArrayList[util.Map[String,Object]] ={
    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }
    try {
      logger.info(s"开始查询task_log_instance")
      val query_sql=s"select * from task_log_instance where id="+id+""
      val stat_query=connection.prepareStatement(query_sql)
      val resultSet=stat_query.executeQuery()
      val md = resultSet.getMetaData()
      val list=new util.ArrayList[util.Map[String,Object]]()
      val columnCount = md.getColumnCount()
      while (resultSet.next()) {
        val rowData = new util.HashMap[String, Object]()
        for (i <- 1 to columnCount) {
          rowData.put(md.getColumnName(i), resultSet.getObject(i))
        }
        list.add(rowData)
      }

      return list

    } catch {
      case ex: Exception => {
        logger.error("task_log_instance更新数据时出现错误", ex.getCause)
        throw ex
      }
    }
  }


  def updateTaskInfo(task_logs_id:String,col_map:Map[String, String]): Unit ={
    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }
    var col_new_map = col_map
    if(!col_map.contains("update_time")){
      col_new_map=col_map.+("update_time"->DateUtil.getCurrentTime())
    }

    try {

      
      println(s"开始更新任务实例:${task_logs_id}")
      var up_sql = col_new_map.map(m=>m._1+"='"+m._2+"'").mkString(",")
      var sql = s"update task_log_instance set "+ up_sql+ " where id="+task_logs_id
      println(s"更新数据如下:${sql}")
      val statement2 = connection.prepareStatement(sql)
      statement2.execute()
      statement2.close()

      println(s"完成更新任务实例:${task_logs_id}")
    } catch {
      case ex: Exception => {
        println("task_log_instance更新数据时出现错误", ex.getCause)
        throw ex
      }
    }

  }

  def updateTaskStatus2(task_logs_id:String,quartzJobInfo_job_id:String,dispatchOption: Map[String, Any],etl_date:String): Unit ={
    var status = "error"
    try{
      var msg = "ETL任务失败存在问题,重试次数已达到最大,状态设置为error"
      if (dispatchOption.getOrElse("plan_count","3").toString.equalsIgnoreCase("-1") || dispatchOption.getOrElse("plan_count","3").toString.toLong > dispatchOption.getOrElse("count","1").toString.toLong) { //重试
        status = "wait_retry"
        msg = "ETL任务失败存在问题,状态设置为wait_retry等待重试"
        if (dispatchOption.getOrElse("plan_count","3").toString.equalsIgnoreCase("-1")) msg = msg + ",并检测到重试次数为无限次"
      }
      logger.info(msg)
      val interval_time = if(dispatchOption.getOrElse("interval_time","").toString.equalsIgnoreCase("")) 5 else dispatchOption.getOrElse("interval_time","5").toString.toInt
      val retry_time=DateUtil.add(new Timestamp(new Date().getTime()),Calendar.SECOND,interval_time);
      if (connection == null){
        synchronized {
          logger.info("数据库未初始化连接,尝试初始化连接")
          if (connection == null) {
            if(!getConnect())
              throw new Exception("connection mariadb fail,could not get connection ")
          }
          logger.info("数据库完成初始化连接")
        }
      }

      if (connection != null && !connection.isValid(5000)) {
        logger.info("数据库连接失效,尝试重新连接")
        connection.close()
        if (!getConnect())
          throw new Exception("connection mariadb fail,could not get connection ")
        logger.info("数据库连接失效,重连成功")
      }

      if(status.equals("wait_retry")){
        var sql3 = s"update task_log_instance set status=?,retry_time=?,update_time= ? where job_id=? and etl_date=? and id=?"
        
        val statement2 = connection.prepareStatement(sql3)
        statement2.setString(1, status)
        statement2.setTimestamp(2, retry_time)
        statement2.setTimestamp(3, new Timestamp(new Date().getTime))
        statement2.setString(4, quartzJobInfo_job_id)
        statement2.setString(5, etl_date)
        statement2.setString(6, task_logs_id)
        statement2.execute()
        statement2.close()

//        val sql = s"update quartz_job_info set last_status=? where job_id=?"
//        val statement = connection.prepareStatement(sql)
//        statement.setString(1, status)
//        statement.setString(2, quartzJobInfo_job_id)
//        statement.execute()
//        statement.close()
        
      }else{
        updateTaskInfo(task_logs_id, Map("status"->status))
      }
    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
        println("task_log_instance更新状态,重试时间出错", ex.getCause)
        updateTaskInfo(task_logs_id, Map("status"->status))
      }
    }



  }



  /**
    * 获取数据库连接
    */
  def getConnect(): Boolean = {

    val props = new Properties();
    var inStream = this.getClass.getResourceAsStream("/datasources.properties")
    if(!config_path.equals("")){
      inStream = new FileInputStream(config_path+"/datasources.properties")
    }

    props.load(inStream)
    if(props.getProperty("enable").equals("false")){
      println("读取配置文件datasources.propertites,但是发现未启用此配置enable is false")
      logger.info("读取配置文件datasources.propertites,但是发现未启用此配置enable is false")
      return false
    }

    val url = props.getProperty("url")
    //驱动名称
    val driver = props.getProperty("driver")
    //用户名
    val username = props.getProperty("username")
    //密码
    val password = props.getProperty("password")


    try {
      //注册Driver
      Class.forName(driver)
      //得到连接
      connection = DriverManager.getConnection(url, username, password)
      true
    } catch {
      case ex: Exception => {
        logger.error("连接数据库时出现错误", ex.getCause)
        throw ex
      }
    }
  }

}

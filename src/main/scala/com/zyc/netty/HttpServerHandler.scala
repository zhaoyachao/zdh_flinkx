package com.zyc.netty

import java.net.URLDecoder
import java.util.Date
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.zyc.base.util.JsonUtil
import com.zyc.common.MariadbCommon
import com.zyc.zdh.DataSources
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil
import org.apache.log4j.MDC
import org.slf4j.LoggerFactory


class HttpServerHandler extends ChannelInboundHandlerAdapter with HttpBaseHandler {
  val logger = LoggerFactory.getLogger(this.getClass)


  //单线程线程池，同一时间只会有一个线程在运行,保证加载顺序
  private val threadpool = new ThreadPoolExecutor(
    1, // core pool size
    1, // max pool size
    500, // keep alive time
    TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable]()
  )

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    println("接收到netty 消息:时间" + new Date(System.currentTimeMillis()))

    val request = msg.asInstanceOf[FullHttpRequest]
    val keepAlive = HttpUtil.isKeepAlive(request)
    val response = diapathcer(request)
    if (keepAlive) {
      response.headers().set(Connection, KeepAlive)
      ctx.writeAndFlush(response)
    } else {
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    ctx.writeAndFlush(defaultResponse(serverErr)).addListener(ChannelFutureListener.CLOSE)
    //    logger.error(cause.getMessage)
    //    logger.error("error:", cause)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush
  }

  /**
    * 分发请求
    *
    * @param request
    * @return
    */
  def diapathcer(request: FullHttpRequest): HttpResponse = {
    val uri = request.uri()
    //数据采集请求
    val param = getReqContent(request)

    if (uri.contains("/api/v1/kill")){
      val task_logs_id=param.getOrElse("task_logs_id", "001").toString
      val dispatch_task_id = param.getOrElse("job_id", "001").toString
      MDC.put("job_id", dispatch_task_id)
      MDC.put("task_logs_id",task_logs_id)
      val r= kill(param)
      MDC.remove("job_id")
      MDC.remove("task_logs_id")
      return r
    }


    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    val dispatch_task_id = dispatchOptions.getOrElse("job_id", "001").toString
    val task_logs_id=param.getOrElse("task_logs_id", "001").toString
    val etl_date = JsonUtil.jsonToMap(dispatchOptions.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString
    try {
      MDC.put("job_id", dispatch_task_id)
      MDC.put("task_logs_id",task_logs_id)
      logger.info(s"接收到请求uri:$uri")
      //MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "etl", etl_date, "22")
      logger.info(s"接收到请求uri:$uri,参数:${param.mkString(",").replaceAll("\"", "")}")
      if (uri.contains("/api/v1/zdh/sql")) {
        sqlEtl(param)
      } else {
        defaultResponse(noUri)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        //MariadbCommon.updateTaskStatus2(task_logs_id,dispatch_task_id,dispatchOptions,etl_date)
        defaultResponse(noUri)
      }
    } finally {
      MDC.remove("job_id")
      MDC.remove("task_logs_id")
    }

  }

  private def getBody(content: String): Map[String, Any] = {
    JsonUtil.jsonToMap(content)
  }

  private def getParam(uri: String): Map[String, Any] = {
    val path = URLDecoder.decode(uri, chartSet)
    val cont = uri.substring(path.lastIndexOf("?") + 1)
    if (cont.contains("="))
      cont.split("&").map(f => (f.split("=")(0), f.split("=")(1))).toMap[String, Any]
    else
      Map.empty[String, Any]
  }

  private def getReqContent(request: FullHttpRequest): Map[String, Any] = {
    request.method() match {
      case HttpMethod.GET => getParam(request.uri())
      case HttpMethod.POST => getBody(request.content.toString(CharsetUtil.UTF_8))
    }
  }

  private def kill(param: Map[String, Any]): DefaultFullHttpResponse={
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString
    val jobGroups=param.getOrElse("jobGroups",List.empty[String]).asInstanceOf[List[String]]


    logger.info(s"开始杀死任务:${jobGroups.mkString(",")}")

    logger.info(s"完成杀死任务:${jobGroups.mkString(",")}")

    defaultResponse(cmdOk)
  }


  private def sqlEtl(param: Map[String, Any]): DefaultFullHttpResponse = {
    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString

    //输入数据源信息
    val dsi_Input = param.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //etl任务信息
    val sqlTaskInfo = param.getOrElse("sqlTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输入数据源类型
    val inPut = dsi_Input.getOrElse("data_source_type", "").toString

    //输入数据源基础信息
    val inPutBaseOptions = dsi_Input

    //输入数据源其他信息k:v,k1:v1 格式
    val inputOptions: Map[String, Any] = sqlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val inputCols: Array[String] = sqlTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output

    //过滤条件
    val filter = sqlTaskInfo.getOrElse("data_sources_filter_input", "").toString


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = sqlTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }


    //清空语句
    val clear = sqlTaskInfo.getOrElse("data_sources_clear_output", "").toString


    threadpool.execute(new Runnable() {
      override def run() = {
        try {
          DataSources.DataHandlerSql(task_logs_id, dispatchOptions, sqlTaskInfo, inPut, inPutBaseOptions ++ inputOptions, outPut,
                      outPutBaseOptions ++ outputOptions, null, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
    defaultResponse(cmdOk)
  }

}

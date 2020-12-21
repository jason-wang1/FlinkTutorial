package com.wb.scala.wc

import java.net.URL

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

case class WordCnt(word: String, cnt: Int)
/**
  * Descreption: XXXX<br/>
  * Date: 2020年12月21日
  *
  * @author WangBo
  * @version 1.0
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {

//    val params = ParameterTool.fromArgs(args)
//    val host: String = params.get("host")
//    val port: Int = params.getInt("port")

    // 创建一个流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    env.disableOperatorChaining()

//    // 接收socket数据流
//    val inputDataStream = env.socketTextStream(host, port)

    // 从文件中读取数据
    val inputPathUrl: URL = this.getClass.getResource("/hello.txt")
    val inputDataStream: DataStream[String] = env.readTextFile(inputPathUrl.getPath)

    // 逐一读取数据，分词之后进行wordcount
    val wordCountDataStream = inputDataStream
      .flatMap(_.split("\\s+"))
      .map( WordCnt(_, 1) )
      .keyBy("word")
      .sum("cnt")

    // 打印输出
    wordCountDataStream.print().setParallelism(1)

    // 执行任务
    env.execute("stream word count job")
  }
}

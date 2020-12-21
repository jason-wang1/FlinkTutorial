package com.wb.scala.wc

import java.net.URL
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Descreption: XXXX<br/>
  * Date: 2020年12月21日
  *
  * @author WangBo
  * @version 1.0
  */
// 批处理代码
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPathUrl: URL = this.getClass.getResource("/hello.txt")
    val inputDataSet = env.readTextFile(inputPathUrl.getPath)

    // 分词之后做count
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map( (_, 1) )
      .groupBy(0)
      .sum(1)

    // 打印输出
    wordCountDataSet.print()
  }
}

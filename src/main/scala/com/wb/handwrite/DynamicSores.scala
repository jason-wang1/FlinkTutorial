package com.wb.handwrite

import java.net.URL

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Descreption: 物品动态得分，不去重
  *
  * Date: 2020年12月23日
  *
  * @author WangBo
  * @version 1.0
  */
case class UserAction(userId: String, itemId: Int, action: String)
object DynamicSores {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputPathUrl: URL = this.getClass.getResource("/user_behavior.txt")
    val inputDataStream: DataStream[String] = env.readTextFile(inputPathUrl.getPath)

    val dataStream: DataStream[UserAction] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        UserAction(arr(0), arr(1).toInt, arr(2))
      })

    val clickRateStream: DataStream[(Int, Double)] =
      dataStream
      .keyBy("itemId")
      .flatMap(new ClickRate())

    clickRateStream.print().setParallelism(1)

    // 执行任务
    env.execute("stream score job")

  }
}

class ClickRate() extends RichFlatMapFunction[UserAction, (Int, Double)] {
  lazy val sum: ValueState[(Long, Long)] = getRuntimeContext.getState(
    new ValueStateDescriptor[(Long, Long)]("show_and_click", classOf[(Long, Long)])
  )

  override def flatMap(value: UserAction, out: Collector[(Int, Double)]): Unit = {
    val tempShowClickNum: (Long, Long) = sum.value()

    val showClickNum = if (tempShowClickNum == null){
      (0L, 0L)
    } else {
      tempShowClickNum
    }

    val newNum = value.action match {
      case "show" => (showClickNum._1 + 1, showClickNum._2)
      case "click" => (showClickNum._1, showClickNum._2 + 1)
    }

    sum.update(newNum)
    if (newNum._1 > 0)
      out.collect((value.itemId, newNum._2/newNum._1.toDouble))
  }
}

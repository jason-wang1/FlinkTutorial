package com.wb.handwrite

import java.net.URL

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Descreption: 每隔5分钟输出最近一小时内点击量最多的前N个商品
  *
  * Date: 2020年12月22日
  *
  * @author WangBo
  * @version 1.0
  */
// 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
case class UserClick(userId: Long, itemId: Long, timestamp: Long)
// 定义窗口聚合结果样例类
case class ItemViewCount(var itemId: Long, windowEnd: Long, var count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputPathUrl: URL = this.getClass.getResource("/UserBehavior.csv")
    val inputDataStream: DataStream[String] = env.readTextFile(inputPathUrl.getPath)

    val dataStream: DataStream[UserClick] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .filter(_.behavior == "pv")
      .map((behavior: UserBehavior) => {
        UserClick(behavior.userId, behavior.itemId, behavior.timestamp)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val windowedStream: WindowedStream[UserClick, Tuple, TimeWindow] =
      dataStream
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.seconds(5))

    val aggStream: DataStream[ItemViewCount] = windowedStream
      .aggregate(new CountAgg(), new PvProcess())

    aggStream.print().setParallelism(1)

    // 执行任务
    env.execute("stream word count job")
  }
}

class CountAgg() extends AggregateFunction[UserClick, Long, Long] {
  override def createAccumulator(): Long = {
    0L
  }

  override def add(in: UserClick, acc: Long): Long = {
    acc + 1
  }

  override def getResult(acc: Long): Long = {
    acc
  }

  override def merge(acc: Long, acc1: Long): Long = {
    acc + acc1
  }
}

class PvProcess() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

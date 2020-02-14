package com.tosin.flink.wordcount

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * sacal版本WordCountStream
 *
 */
object WordCountStreamScala {
  def main(args: Array[String]): Unit = {
    //1. 获取执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 加载、创建初始的数据（source）
    val source: DataStream[String] = environment.socketTextStream("bd-01-01", 5000, '\n')
    //3. 数据转换 transformation
    //Error:(18, 58) could not find implicit value for evidence parameter of type org.apache.flink.api.common.typeinfo.TypeInformation[String]
    //    val wordCount: DataStream[WordCount] = source.flatMap(f => f.split(" "))
    import org.apache.flink.streaming.api.scala._
    val wordCount: DataStream[WordCount] = source.flatMap(f => f.split(" "))
      .map(m => WordCount(m, 1L))
      .keyBy("word")
      //窗口的长度3s，滑动时间2s
      .timeWindow(Time.seconds(5), Time.seconds(2))
      .sum("count")
    //4. 结果输出 sink
    wordCount.print()
    //5. 触发程序的计算
    environment.execute()
  }

  case class WordCount(word:String, count:Long)
}

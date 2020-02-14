package com.tosin.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 2. 流式
 * 接收端口中的数据，流式的计算，使用时间窗口，窗口的长度3s，滑动时间1s
 * 测试
 * 1. 启动监听端口
 * # nc -l bd-01-01 5000
 * [root@bd-01-01 ~]# nc -l 5000
 * a
 * b
 * b
 * b
 * 2. 启动代码
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. 加载、创建初始的数据（source）
        //数据源是端口中的,第一个参数是主机名、IP，第二参数端口、第三个是分隔符
        DataStreamSource<String> stringDataStreamSource = streamExecutionEnvironment.socketTextStream("bd-01-01", 5000, "\n");
        //3. 数据转换 transformation
        SingleOutputStreamOperator<WordCount> wordCount = stringDataStreamSource.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String line, Collector<WordCount> collector) throws Exception {
                for(String s: line.split(" ")){
                    collector.collect(new WordCount(s, 1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(3), Time.seconds(1)).sum("count");
        //4. 结果输出 sink
        wordCount.print();
        //5. 触发程序的计算
        streamExecutionEnvironment.execute();
    }

    public static class WordCount {
        private String word;
        private long count;

        //无参构造必须存在 否则:Exception in thread "main" org.apache.flink.api.common.InvalidProgramException: This type (GenericType<com.tosin.flink.wordcount.WordCountStream.WordCount>) cannot be used as key.
        public WordCount() {}
        public WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public void setWord(String word) {
            this.word = word;
        }
        public void setCount(long count) {
            this.count = count;
        }

        public String getWord() {
            return word;
        }
        public long getCount() {
            return count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}

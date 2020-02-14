package com.tosin.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
//import scala.Tuple2;

/**
 * 1. 批处理
 * Dataset API。对静态数据进行批处理，将静态数据抽象成分布式数据集，使用flink提供的操作对分布式数据集进行处理（相对spark core）
 * 遵循 Flink的编程模型
 * 每个flink程序或包含若干步骤：
 * 	1. 获取执行环境
 * 	2. 加载、创建初始的数据（source）
 * 	3. 数据转换 transformation
 * 	4. 结果输出 sink
 * 	5. 触发程序的计算
 */
public class WordCountDataSet {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        //2. 加载、创建初始的数据（source）
        DataSource<String> stringDataSource = executionEnvironment.fromElements("hello flink DataSet stream JAVA JAVA scala flink");
//        DataSet<String> stringDataSource = executionEnvironment.fromElements("hello flink DataSet stream JAVA JAVA scala flink");
        //3. 数据转换 transformation
        //按第一元素分组，按第二个元素求和
        AggregateOperator<Tuple2<String, Integer>> wordcount = stringDataSource.flatMap(new LineSplit()).groupBy(0).sum(1);
//        DataSet<Tuple2<String, Integer>> wordcount = stringDataSource.flatMap(new LineSplit()).groupBy(0).sum(1);
        //4. 结果输出 sink
        wordcount.print();
        //5. 触发程序的计算
    }

    private static class LineSplit implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for(String s: line.split(" ")){
                collector.collect(new Tuple2<String, Integer>(s, 1));
            }
        }
    }
}

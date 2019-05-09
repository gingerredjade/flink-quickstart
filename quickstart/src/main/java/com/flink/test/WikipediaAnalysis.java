package com.flink.test;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * 实时监控维基百科词条有哪些用户在修改，他们修改了多少内容等等，在这里统计的是字节数
 */
public class WikipediaAnalysis {

    public static void main(String[] args) throws Exception {

        // 创建一个streaming程序运行的上下文
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        //see.setParallelism(5);
        System.out.println("當前的並行度：["+see.getParallelism()+"]");

        /*
        source部分--数据来源部分
        给上下文添加一个source。pom中引入flink-connector-wikiedits_2.11，它已经封装好了WikipediaEditsSource，直接创建WikipediaEditsSource实例即可。
         */
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        /*
        edits这个输入要有个key，表示某一个时间段之内每一个用户
        所以这里要用keyBy,用一个Key选择器KeySelector返回每一条event(代表维基百科后台每一条修改的事件)，从这个事件中把用户拿出来
        现在就得到了一个keyedEdits。
         */
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy((KeySelector<WikipediaEditEvent, String>) event -> {
                    return event.getUser();
                });

        /*
        因为现在是一个没有边界的流输入环境中，要有一个窗口，期望这个窗口是5s汇总一次。
        然后可以直接调用TimeWindow，指定窗口的宽度是5s。这样每过5s，event发送到窗口中，我们对这个时间窗口内的数据进行处理。
        调用fold这个聚合函数，它有两个输入initialValue、function。我们既然统计的是每一个用户以及他编辑的字节数，我们需要一个初始值。
        这个初始值是一个Tuple，有两个字段，第一个字段是user，现在直接给user赋值一个空串，；第二个字段是这个user所编辑的字节数，用long型0代替作为它初始值。
        第二个参数foldFunction，直接实例化一个
         */
        DataStream<Tuple2<String, Long>> result =  keyedEdits
                .timeWindow(Time.seconds(5)) // 指定窗口的宽度为5秒
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> tuple2, WikipediaEditEvent o) throws Exception {
                        /*
                        这里o是输入，tuple2是输出
                        WikipediaEditEvent窗口中每一条信息就是维基百科的编辑信息
                        把user拿到赋值给tuple2第一个字段-表示用户
                        把bytediff拿到赋值给tuple2第二个字段-表示该用户在5s时间窗口内所编辑的字节数
                         */
                        tuple2.f0 = o.getUser();
                        Long byteDiff = Long.valueOf(o.getByteDiff());
                        tuple2.f1 += byteDiff;
                        return tuple2;
                    }
                });

        // 这里没有sink部分，直接做一个打印，实际生产环境，这里可能是发送到Kafka/落地到其他存储Redis等
        result.print();
        // 用上下文进行执行
        see.execute();
    }
}

/**
 * 本地直接运行这个main方法，就可以在本地启动一个Flink环境，并将它提交。
 *
 * 跟进print()函数->PrintSinkFunction的open()->PrintSinkOutputWriter的open()函数，会发现
 * 当你的数据并行度大于1时，会有处理
 * 当sinkIdentifier不为空时，前缀增加">"标识，即表示第几个subtask。
 */
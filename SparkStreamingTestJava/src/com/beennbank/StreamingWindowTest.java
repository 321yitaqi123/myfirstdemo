package com.beenbank;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class StreamingWindowTest {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("testStream").setMaster("local[2]");
        // JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext js = new JavaStreamingContext(conf, Durations.seconds(5));
        //JavaReceiverInputDStream<String> lines = js.socketTextStream("localhost", 9999);
       js.checkpoint("./checkpoint");
        js.sparkContext().setLogLevel("ERROR");

        //首先创建一个kafka的参数
        Map<String, String> kafkaParams = new HashMap<String, String>();
        //此处存储boker.list
        kafkaParams.put("metadata.broker.list", "172.16.22.176:9092");
        //可以读取多个topic
//		kafkaParameters.put("auto.offset.reset", "smallest");

        HashSet<String> topics = new HashSet<String>();
        topics.add("test");
        JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(js,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);



        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(Tuple2<String, String> t) throws Exception {
                return  Arrays.asList(t._2.split(" ")).iterator();
            }
        });

       /* JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<String> call(String s) throws Exception {
               return Arrays.asList(s.split(" ")).iterator();
            }
        });*/

        JavaPairDStream<String, Integer> paird = words.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        JavaPairDStream<String, Integer> counts = paird.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        JavaPairDStream<String, Integer> counts2 = paird.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        },Durations.seconds(15), Durations.seconds(5));

        counts2.print();
        js.start();
        try {
            js.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}

package com.beenbank;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.sql.ResultSet;
import java.util.*;

public class StreamingTest {

    public static void main(String[] args) throws InterruptedException {


       // System.setProperty("hadoop.home.dir", "C:\\Users\\admin\\Desktop\\work\\hadoop-2.7.5");

        //SparkConf conf = new SparkConf().setAppName("testStream").setMaster("local[2]");
        SparkConf conf = new SparkConf().setAppName("testStream");
       // JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext js = new JavaStreamingContext(conf, Durations.seconds(5));
        //JavaReceiverInputDStream<String> lines = js.socketTextStream("localhost", 9999);



       //首先创建一个kafka的参数
        Map<String, String> kafkaParams = new HashMap<String, String>();
        //此处存储boker.list
        kafkaParams.put("metadata.broker.list", "172.16.22.176:9092");
        kafkaParams.put("auto.offset.reset", "largest");
        //可以读取多个topic
//		kafkaParameters.put("auto.offset.reset", "smallest");

        HashSet<String> topics = new HashSet<String>();
        topics.add("test7");
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
        //counts.print();

        counts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(JavaPairRDD<String, Integer> counts) throws Exception {
                final Map<String ,Integer> words = new HashMap<String,Integer>();
                List<Object[]> insertParams = new ArrayList<Object[]>();
                List<Object[]> updateParams = new ArrayList<Object[]>();
                Map<String ,Integer> newWords = counts.collectAsMap();
                /*counts.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> partition) throws Exception {

                        while (partition.hasNext()){
                            Tuple2<String, Integer> count = partition.next();
                            if (count._1  != null){
                            newWords.put(count._1,count._2);
                            }else{newWords.put("+null+",count._2);}
                        }
                    }
                });*/
                JDBCUtil jdbc = JDBCUtil.getJDBCInstance();
                String sql ="select * from wordcount";

                jdbc.doQuery(sql, null, new ExecuteCallBack() {
                    @Override
                    public void resultCallBack(ResultSet result) throws Exception {
                        while(result.next()){
                           String  word = result.getString("word");
                           int count = result.getInt("count");
                           words.put(word,count);

                        }
                    }
                });
                for ( Map.Entry<String, Integer> entry : newWords.entrySet()){
                    String word  = entry.getKey();
                    Integer val = entry.getValue();
                    Integer oldVal =words.get(word);
                    if(oldVal != null ){
                        updateParams.add(new Object[]{oldVal+val,word});

                    }else {
                        insertParams.add(new Object[]{word,val});

                    }
                }
                String insertSQL = "insert into wordcount values(?,?)";
                String  updateSQL = "update  wordcount set count=? where word=?";
                jdbc.doBatch(insertSQL,insertParams);
                jdbc.doBatch(updateSQL,updateParams);

            }
        });


        js.start();
        try {
            js.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}

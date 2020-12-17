package com.beenbank;

import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;

public class StreamingTestLogs {


  /* private final static String  CONSUMER_TOPIC = "error_log";
    private final  static String  CLIENT_ID ="error_log";
    private final  static String GROUP_ID ="group3";
    private final  static String CONSUMER_ID="consum3";
    private final  static String KAFKA_BROKER_LIST="172.16.22.176:9092";//逗号分隔
    private final  static String ZK_CONNECT ="172.16.22.176:2181";
    private final  static String ZK_CONNECT_TIMEOUT ="6000";
    private final  static String ZK_SESSION_TIMEOUT ="6000";
    private final  static String STREAMING_APPNAME="testStreamLog";
    private final  static Long STREAMING_DURATION=5L;
    private final  static String HDFS_FILEPATH ="hdfs://10.88.20.123:9000/spark/appname.property";
    private final  static String testfilepath ="./bakc.txt";
    private  final  static String APPNAME_SEPARATOR =":";
    private  final  static String SYSTEM_SUM_SEPARATOR ="=";
    private final  static String SIMPLE_CONSUMER_HOST ="172.16.22.176";
    private  final  static int SIMPLE_CONSUMER_PORT =9092;
    private final  static String JAVAAPP_ERROR_KEYWORD ="#@#";
    private final  static String NGINX_ERROR_KEYWORD ="#~#";
    private final  static String NGINX_ACCESS_KEYWORD ="#!@!#";
    private final  static String PHP_ERROR_KEYWORD ="#~@~#";*/


     private final  static String  CONSUMER_TOPIC = "error_log";
        private final  static String  CLIENT_ID ="error_log";
        private final  static String GROUP_ID ="grouptest1";
       private  final  static String CONSUMER_ID="consumtest1";
       private  final  static String KAFKA_BROKER_LIST="172.16.0.109:9092";//逗号分隔
        private final  static String ZK_CONNECT ="172.16.0.109:2181";
        private final  static String ZK_CONNECT_TIMEOUT ="6000";
       private  final  static String ZK_SESSION_TIMEOUT ="6000";
       private  final  static String STREAMING_APPNAME="testStreamLog";
        private final  static Long STREAMING_DURATION=5L;
        private final  static String HDFS_FILEPATH ="hdfs://iZbp15gop3bkpn1bf4c4lcZ:9000/spark/appname.property";
        private final  static String APPNAME_SEPARATOR =":";
        private  final  static String SYSTEM_SUM_SEPARATOR ="=";
        private final  static String SIMPLE_CONSUMER_HOST ="172.16.0.109";
        private final  static int SIMPLE_CONSUMER_PORT =9092;
        private  final  static String JAVAAPP_ERROR_KEYWORD ="#@#";
         private final  static String NGINX_ERROR_KEYWORD ="#~#";
        private  final  static String NGINX_ACCESS_KEYWORD ="#!@!#";
        private final  static String PHP_ERROR_KEYWORD ="#~@~#";



    public static void main(String[] args) {

        //本地运行设置
   //System.setProperty("hadoop.home.dir", "C:\\Users\\admin\\Desktop\\work\\hadoop-2.7.5");
  // SparkConf conf = new SparkConf().setAppName(STREAMING_APPNAME).setMaster("local[2]");
        //yarn环境设置
     SparkConf conf = new SparkConf().setAppName(STREAMING_APPNAME);
        JavaSparkContext jsc =new JavaSparkContext(conf) ;
        final JavaStreamingContext js = new JavaStreamingContext(jsc, Durations.seconds(STREAMING_DURATION));
            //JavaRDD<String>  sysname = js.sparkContext().textFile(testfilepath);
              Map<String ,String> appSys =null;
              try{
                 JavaRDD<String>  sysname = jsc.textFile(HDFS_FILEPATH);
                  appSys= sysname.mapToPair(new PairFunction<String,String,String>() {

                    @Override
                    public Tuple2 call(String o) {
                        if (o.split(APPNAME_SEPARATOR).length ==2){
                            return new Tuple2<String,String>(o.split(APPNAME_SEPARATOR)[0],o.split(APPNAME_SEPARATOR)[1]);
                        }
                        return new Tuple2<String,String>(null,"1");
                    }
                }).collectAsMap();
              }catch (Exception e){
                  System.out.println(e);
              }
                //广播变量
                final  Broadcast<Map<String,String>>  bord = js.sparkContext().broadcast(appSys);

       //首先创建一个kafka的参数
        Map<String, String> kafkaParams = new HashMap<String, String>();
        //此处存储boker.list，，分割
        kafkaParams.put("metadata.broker.list", KAFKA_BROKER_LIST);
        //kafkaParams.put("auto.offset.reset", "largest");
       // 消费者组
        kafkaParams.put("group.id", GROUP_ID);
        //消费者
        kafkaParams.put("consumer.id", CONSUMER_ID);
        // kafkaParams.put("client.id ", "group1 ");
        // kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("zookeeper.connect", ZK_CONNECT);
        kafkaParams.put("zookeeper.session.timeout.ms", ZK_SESSION_TIMEOUT);
        kafkaParams.put("zookeeper.connection.timeout.ms", ZK_CONNECT_TIMEOUT);
        // kafkaParams.put("zookeeper.sync.time.ms ", "2000");
       // kafkaParams.put("auto.commit.enable", "true");
       // kafkaParams.put("auto.commit.interval.ms", "5000");
        kafkaParams.put("fetch.message.max.bytes", "52428800");

        //可以读取多个topic
       // kafkaParams.put("auto.offset.reset", "smallest");

       /* HashSet<String> topics = new HashSet<String>();
        topics.add("test7");
        JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(js,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);*/
        //val topic="kafka_test4"
        //查看当前topic：__consumer_offsets中已存储的最新的offset
        SimpleConsumer simpleConsumer = new SimpleConsumer(SIMPLE_CONSUMER_HOST, SIMPLE_CONSUMER_PORT, 1000000, 64 * 1024, CLIENT_ID);//new一个consumer并连接上kafka
        //val topiclist=Seq("kafka_test4")
        List<String> topiclist = new ArrayList<String>();
        topiclist.add(CONSUMER_TOPIC);
        TopicMetadataRequest topicReq = new TopicMetadataRequest(topiclist,0);//定义一个topic请求，为了获取相关topic的信息（不包括offset,有partition）
        TopicMetadataResponse res = simpleConsumer.send(topicReq);//发送请求，得到kafka相应

        List<TopicMetadata> topicMetaOption = res.topicsMetadata();
        List<TopicAndPartition> topicAndPartition = new ArrayList<TopicAndPartition>();

        for( TopicMetadata tmp :topicMetaOption) {
            for (PartitionMetadata pt : tmp.partitionsMetadata()){

                topicAndPartition.add(new TopicAndPartition(tmp.topic(), pt.partitionId()));
            }
        }
        OffsetFetchRequest fetchRequest =new  OffsetFetchRequest(GROUP_ID,topicAndPartition,(short)0,0,CLIENT_ID);
        //val fetchRequest = OffsetFetchRequest("user3",topicAndPartition)//定义一个请求，传递的参数为groupid,topic,partitionid,这三个也正好能确定对应的offset的位置

        Map<TopicAndPartition,OffsetMetadataAndError> offsets = simpleConsumer.fetchOffsets(fetchRequest).offsets();
        // val fetchResponse = simpleConsumer.fetchOffsets(fetchRequest).requestInfo//向kafka发送请求并获取返回的offset信息


//    println(fetchRequest)
//    println(fetchResponse)
        Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
        // List offsetList = new ArrayList();
        for ( Map.Entry<TopicAndPartition, OffsetMetadataAndError> entry : offsets.entrySet()) {
            Long offs=  entry.getValue().offset();
           // System.out.println(entry.getKey()+"+++++++++++++++"+offs);
            //新消费者消费偏移量为-1
           // if(offs==1565091){offs=1567784L;}
            fromOffsets.put(entry.getKey(),offs);
        }
        try {
            simpleConsumer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        JavaInputDStream<String> lines = KafkaUtils.createDirectStream(js, String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParams,
                fromOffsets, new Function<MessageAndMetadata<String, String>, String>() {
                    @Override
                    public String call(MessageAndMetadata<String, String> v1) {
                        //System.out.println("+++++++++++++++++++1111111111111111++++++++++++++++++++++++");
                        // v1.topic()+v1.partition()+v1.offset()+v1.message();
                        // "topc"+v1.topic()+"partition "+v1.partition()+"offset "v1.offset()+"messag "+v1.message();
                        return "topic"+v1.topic()+"partition"+v1.partition()+"offset"+v1.offset()+"messag "+v1.message();
                    }
                });
                lines.persist(StorageLevel.MEMORY_AND_DISK());

        JavaDStream<String> javaapplogs = lines.filter(new Function<String, Boolean>() {
           @Override
           public Boolean call(String s) {
              // System.out.println(s);
               return s.contains(JAVAAPP_ERROR_KEYWORD);
           }
       });
        JavaDStream<String> nginxerror = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {

                return s.contains(NGINX_ERROR_KEYWORD);
            }
        });
        JavaDStream<String> nginxaccess = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return s.contains(NGINX_ACCESS_KEYWORD);
            }
        });

        JavaDStream<String> phperror = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return s.contains(PHP_ERROR_KEYWORD);
            }
        });

        JavaPairDStream<String, Integer> phpErrorDS = phperror.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String line) {
                String appName=null;
                String appNameAndTime =null;
                String errTime=null;

                if (line != null){
                    String[] messages =line.substring(line.indexOf("messag")+6).split(PHP_ERROR_KEYWORD);

                    if(messages.length<5){
                        return new Tuple2<String, Integer>(null,1);
                    }

                    appName=messages[1];
                    if (messages[3].length()>8){
                        errTime = messages[3].substring(0,8);
                    }
                }
                if (errTime != null && appName != null){
                    appNameAndTime =appName+PHP_ERROR_KEYWORD+errTime;

                }
                //System.out.println(appNameAndTime);
                Map<String ,String> sysmap = null;
                if(bord != null){
                    sysmap =bord.value();
                }

                if( sysmap!= null && appNameAndTime != null && sysmap.get(appName.trim()) !=null){
                    if (sysmap.get(appName).split(SYSTEM_SUM_SEPARATOR).length==2){
                        return new Tuple2<String, Integer>(sysmap.get(appName).split(SYSTEM_SUM_SEPARATOR)[1]+PHP_ERROR_KEYWORD+appNameAndTime,1);
                    }
                    return new Tuple2<String, Integer>(sysmap.get(appName)+PHP_ERROR_KEYWORD+appNameAndTime,1);
                }else{
                    return new Tuple2<String, Integer>(appName+PHP_ERROR_KEYWORD+appNameAndTime,1);}


            }
        });

        JavaPairDStream<String, Integer> nginxAccessDS = nginxaccess.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String line) {
                String appName=null;
                String accessGrade =null;
                String appNameGradeTime =null;
                String errTime=null;

                if (line != null){
                    String[] messages =line.substring(line.indexOf("messag")+6).split(NGINX_ACCESS_KEYWORD);

                    if(messages.length<8){
                        return new Tuple2<String, Integer>(null,1);
                    }
                    accessGrade =messages[7].substring(0,1)+"00";
                    appName=messages[1];
                    if (messages[5].length()>8){
                        errTime = messages[5].substring(0,8);
                    }
                }
                if (errTime != null && appName != null){
                    appNameGradeTime =appName+NGINX_ACCESS_KEYWORD+accessGrade+NGINX_ACCESS_KEYWORD+errTime;

                }else{return new Tuple2<String, Integer>(null,1);}

                Map<String ,String> sysmap =null;

                if (bord != null){
                    sysmap =bord.value();
                }

                if( sysmap!= null &&  sysmap.get(appName.trim()) !=null){
                    return new Tuple2<String, Integer>(sysmap.get(appName)+NGINX_ACCESS_KEYWORD+appNameGradeTime,1);
                }else{
                    return new Tuple2<String, Integer>(appName+SYSTEM_SUM_SEPARATOR+appName+NGINX_ACCESS_KEYWORD+appNameGradeTime,1);}



            }
        });


        JavaPairDStream<String, Integer> nginxErrorDS = nginxerror.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String line) {
                String appName=null;
                String errorGrade =null;
                String appNamegRadeTime =null;
                String errTime=null;
              //  return null;
                if (line != null){
                   String[] messages =line.substring(line.indexOf("messag")+6).split(NGINX_ERROR_KEYWORD);

                   if(messages.length<6){
                        return new Tuple2<String, Integer>(null,1);
                   }
                    errorGrade =messages[2];
                    appName=messages[5];
                   if (messages[1].length()>8){
                    errTime = messages[1].substring(0,8);
                   }
                }
                if ( errTime != null && appName != null && errorGrade != null ){
                    appNamegRadeTime =appName+NGINX_ERROR_KEYWORD+errorGrade+NGINX_ERROR_KEYWORD+errTime;

                }
                //System.out.println(appNameAndTime);

                Map<String ,String> sysmap =null;
                if (bord != null){
                    sysmap =bord.value();
                }

                if( sysmap!= null && appNamegRadeTime != null && sysmap.get(appName.trim()) !=null){
                    return new Tuple2<String, Integer>(sysmap.get(appName)+NGINX_ERROR_KEYWORD+appNamegRadeTime,1);
                }else{
                    return new Tuple2<String, Integer>(appName+SYSTEM_SUM_SEPARATOR+appName+NGINX_ERROR_KEYWORD+appNamegRadeTime,1);}

            }
        });

        JavaDStream<String> logs = javaapplogs.map(new Function<String, String>() {
            @Override
            public String call(String line) {

                String appName=null;
                String appNameAndTime =null;
                String errTime=null;
                if (line != null){
                    String[] messages =line.substring(line.indexOf("messag")+6).split(JAVAAPP_ERROR_KEYWORD);
                    if(messages.length<5){
                        return  null;
                    }
                    appName=messages[1];
                    if (messages[3].length()>8){
                        errTime = messages[3].substring(0,8);
                    }
                }
                if ( errTime != null && appName != null ){
                    appNameAndTime =appName +JAVAAPP_ERROR_KEYWORD+errTime;
                }

                return appNameAndTime;
            }
        });

        JavaPairDStream<String, Integer> paird =logs.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String s) {
                Map<String ,String> sysmap =null;
                if (bord != null){
                     sysmap =bord.value();
                }


               // bord.unpersist();

                if(s == null ){
                    return new Tuple2<String, Integer>(null,1);
                }
                String appName =s.split(JAVAAPP_ERROR_KEYWORD)[0];


               if( sysmap!= null && sysmap.get(appName) !=null ){
                   if (sysmap.get(appName).split(SYSTEM_SUM_SEPARATOR).length==2){
                       return new Tuple2<String, Integer>(sysmap.get(appName).split(SYSTEM_SUM_SEPARATOR)[1]+JAVAAPP_ERROR_KEYWORD+s,1);
                   }
                   return new Tuple2<String, Integer>(sysmap.get(appName)+JAVAAPP_ERROR_KEYWORD+s,1);
               }else{
                   return new Tuple2<String, Integer>(appName+JAVAAPP_ERROR_KEYWORD+s,1);}
            }

        });

        JavaPairDStream<String, Integer> phpErrorCounts = phpErrorDS.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) {
                return v1+v2;
            }
        });

        JavaPairDStream<String, Integer> nginxAccessCounts = nginxAccessDS.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) {
                return v1+v2;
            }
        });

        JavaPairDStream<String, Integer> nginxErrorCounts = nginxErrorDS.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) {
                return v1+v2;
            }
        });
        JavaPairDStream<String, Integer> javaErrorCounts = paird.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) {
                return v1+v2;
            }
        });
       // phpErrorCounts.print();
        JavaPairDStream<String, Integer> counts = javaErrorCounts.union(nginxErrorCounts).union(nginxAccessCounts).union(phpErrorCounts);

       counts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            String date;
            {
                SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
                Calendar calendar = Calendar.getInstance();
                calendar.add(Calendar.DAY_OF_MONTH,-2);
                date = df.format(calendar.getTime());
            }
            @Override
            public void call(JavaPairRDD<String, Integer> counts) throws Exception {

                  if( counts == null || counts.count()  <= 0  ){
                      return;
                  }

                Map<String ,Integer> newlogs = counts.collectAsMap();

                doJavaError( newlogs , date);
                doNginxError( newlogs , date);
                doNginxAccess(newlogs , date);
                doPhpError(newlogs , date);

            }
        });

        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) {
                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> stringIterator) {
                        Long aaa = -1L;
                        int par = -1;
                        String top=null;
                        while (stringIterator.hasNext()){
                            String aa =stringIterator.next();
                             top = aa.substring(aa.indexOf("topic")+5 ,aa.indexOf("partition")).trim();
                             par =Integer.parseInt(aa.substring(aa.indexOf("partition")+9,aa.indexOf("offset")).trim());
                            aaa=Long.valueOf( aa.substring(aa.indexOf("offset")+6,aa.indexOf("messag")));
                        }
                        if(aaa != -1L){
                            SimpleConsumer simpleConsumer2 = new SimpleConsumer(SIMPLE_CONSUMER_HOST, SIMPLE_CONSUMER_PORT, 1000000, 64 * 1024, CLIENT_ID);//new一个consumer并连接上kafka
                            TopicAndPartition topicAndPartition = new TopicAndPartition(top, par);//定义一个格式
                            OffsetAndMetadata mmm = new OffsetAndMetadata(aaa+1,OffsetAndMetadata.NoMetadata(),-1L);
                            Map<TopicAndPartition,OffsetAndMetadata> ttt =  new HashMap<TopicAndPartition,OffsetAndMetadata>();
                            ttt.put(topicAndPartition,mmm);
                            OffsetCommitRequest commitRequest = new OffsetCommitRequest(GROUP_ID, ttt,0,CLIENT_ID,(short)0); //定义一个请求，注意，在这里存储的是fromOffset
                            simpleConsumer2.commitOffsets(commitRequest);
                            if (simpleConsumer2 !=null) try {
                                simpleConsumer2.close();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            // System.out.println("+++++++++++++++topic,partition,offsets "+top +" "+ par +" "+aaa);
                        }
                    }
                });
            }
        });

        js.start();
        try {
            js.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private static void doJavaError(Map<String, Integer> newlogs, String date) throws Exception {

        String sql ="select * from fact_log_sum_platform where atdate > '"+date+"'";
        JDBCUtil jdbc = JDBCUtil.getJDBCInstance();
       final Map<String ,Integer> logs = new HashMap<String,Integer>();
        List<Object[]> insertParams = new ArrayList<Object[]>();
        List<Object[]> updateParams = new ArrayList<Object[]>();
        jdbc.doQuery(sql, null, new ExecuteCallBack() {
            @Override
            public void resultCallBack(ResultSet result) throws Exception {
                while(result.next()){
                    String  word = result.getString("platform_name");
                    String  recordTime = result.getString("atdate");
                    int count = result.getInt("error_log_count");
                    String  collation_name = result.getString("collation_name");
                    logs.put(collation_name+JAVAAPP_ERROR_KEYWORD+word+JAVAAPP_ERROR_KEYWORD+recordTime,count);
                   // System.out.println("rrrrrrr"+word+"#!#"+count);

                }
            }
        });

        for ( Map.Entry<String, Integer> entry : newlogs.entrySet()){
            String appName  = entry.getKey();
            if (appName == null){
                continue;
            }
            Integer val = entry.getValue();
            Integer oldVal =logs.get(appName);

            if(oldVal != null && appName.split(JAVAAPP_ERROR_KEYWORD ).length == 3){

               //.out.println("+++++++++++++++"+appName + "val " +(oldVal+val));
                updateParams.add(new Object[]{oldVal+val,appName.split(JAVAAPP_ERROR_KEYWORD)[1],appName.split(JAVAAPP_ERROR_KEYWORD)[2]});
            }else if (appName.split(JAVAAPP_ERROR_KEYWORD ).length == 3){
                insertParams.add(new Object[]{appName.split(JAVAAPP_ERROR_KEYWORD)[2],appName.split(JAVAAPP_ERROR_KEYWORD)[0],appName.split(JAVAAPP_ERROR_KEYWORD)[1],val,});
               // System.out.println("+++++++++++++++"+appName +"val " +val );
            }
        }

        String insertSQL = "insert into fact_log_sum_platform values(?,?,?,?)";
        String  updateSQL = "update  fact_log_sum_platform set error_log_count=? where platform_name=? and atdate=?";

        jdbc.doBatch(insertSQL,insertParams);
        jdbc.doBatch(updateSQL,updateParams);
    }

    private static void doNginxError(Map<String, Integer> newlogs, String date) throws Exception {

        String sql ="select * from fact_log_sum_nginx_error where atdate > '"+date+"'";
        JDBCUtil jdbc = JDBCUtil.getJDBCInstance();
        final Map<String ,Integer> logs = new HashMap<String,Integer>();
        List<Object[]> insertParams = new ArrayList<Object[]>();
        List<Object[]> updateParams = new ArrayList<Object[]>();
        jdbc.doQuery(sql, null, new ExecuteCallBack() {
            @Override
            public void resultCallBack(ResultSet result) throws Exception {
                while(result.next()){
                    String  word = result.getString("server_name");
                    String  recordTime = result.getString("atdate");
                    String level_name = result.getString("level_name");
                    String  collationName = result.getString("collation_name");
                    String  collationSumName = result.getString("collation_sum_name");
                    int count = result.getInt("log_count");
                    logs.put(collationName+SYSTEM_SUM_SEPARATOR+collationSumName+NGINX_ERROR_KEYWORD+word+NGINX_ERROR_KEYWORD+level_name+NGINX_ERROR_KEYWORD+recordTime,count);

                }
            }
        });
        for ( Map.Entry<String, Integer> entry : newlogs.entrySet()){
            String appName  = entry.getKey();
            if (appName == null){
                continue;
            }
            Integer val = entry.getValue();
            Integer oldVal =logs.get(appName);
            if(oldVal != null && appName.split(NGINX_ERROR_KEYWORD).length == 4){
                updateParams.add(new Object[]{oldVal+val,appName.split(NGINX_ERROR_KEYWORD)[1],appName.split(NGINX_ERROR_KEYWORD)[3],appName.split(NGINX_ERROR_KEYWORD)[2]});
                //System.out.println("+++++++++++++++"+appName + "val " +(oldVal+val));
            }else if (appName.split(NGINX_ERROR_KEYWORD).length == 4){
               //System.out.println("+++++++++++++++"+appName +"  val " +val );
                if(appName.split(NGINX_ERROR_KEYWORD)[0].split(SYSTEM_SUM_SEPARATOR).length != 2){
                    insertParams.add(new Object[]{appName.split(NGINX_ERROR_KEYWORD)[3],appName.split(NGINX_ERROR_KEYWORD)[1],appName.split(NGINX_ERROR_KEYWORD)[1],appName.split(NGINX_ERROR_KEYWORD)[1],appName.split(NGINX_ERROR_KEYWORD)[2],val,});
                }else{
                    insertParams.add(new Object[]{appName.split(NGINX_ERROR_KEYWORD)[3],appName.split(NGINX_ERROR_KEYWORD)[0].split(SYSTEM_SUM_SEPARATOR)[1],appName.split(NGINX_ERROR_KEYWORD)[0].split(SYSTEM_SUM_SEPARATOR)[0],appName.split(NGINX_ERROR_KEYWORD)[1],appName.split(NGINX_ERROR_KEYWORD)[2],val,});
                }
            }
        }
        String insertSQL = "insert into fact_log_sum_nginx_error values(?,?,?,?,?,?)";
        String  updateSQL = "update  fact_log_sum_nginx_error set log_count=? where server_name=? and atdate=? and level_name=?";
        jdbc.doBatch(insertSQL,insertParams);
        jdbc.doBatch(updateSQL,updateParams);
    }

    private static void doNginxAccess(Map<String, Integer> newlogs, String date) throws Exception {

        String sql ="select * from fact_log_sum_nginx_access where atdate > '"+date+"'";
        JDBCUtil jdbc = JDBCUtil.getJDBCInstance();
        final Map<String ,Integer> logs = new HashMap<String,Integer>();
        List<Object[]> insertParams = new ArrayList<Object[]>();
        List<Object[]> updateParams = new ArrayList<Object[]>();
        jdbc.doQuery(sql, null, new ExecuteCallBack() {
            @Override
            public void resultCallBack(ResultSet result) throws Exception {
                while(result.next()){
                    String  word = result.getString("server_name");
                    String  recordTime = result.getString("atdate");
                    String  collationName = result.getString("collation_name");
                    String  collationSumName = result.getString("collation_sum_name");
                    String level_name = result.getString("status_code");
                    int count = result.getInt("log_count");
                    logs.put(collationName+SYSTEM_SUM_SEPARATOR+collationSumName+NGINX_ACCESS_KEYWORD+word+NGINX_ACCESS_KEYWORD+level_name+NGINX_ACCESS_KEYWORD+recordTime,count);
                    // System.out.println("+++++++++++++++"+word+"#!#"+recordTime);
                }
            }
        });
        for ( Map.Entry<String, Integer> entry : newlogs.entrySet()){
            String appName  = entry.getKey();
            if (appName == null){
                continue;
            }
            Integer val = entry.getValue();
            Integer oldVal =logs.get(appName);
            if(oldVal != null && appName.split(NGINX_ACCESS_KEYWORD).length==4){
                updateParams.add(new Object[]{oldVal+val,appName.split(NGINX_ACCESS_KEYWORD)[1],appName.split(NGINX_ACCESS_KEYWORD)[3],appName.split(NGINX_ACCESS_KEYWORD)[2]});
               // System.out.println("+++++++++++++++"+appName + "val " +(oldVal+val));
            }else if (appName.split(NGINX_ACCESS_KEYWORD).length==4){
                if(appName.split(NGINX_ACCESS_KEYWORD)[0].split(SYSTEM_SUM_SEPARATOR).length == 2){
                    insertParams.add(new Object[]{appName.split(NGINX_ACCESS_KEYWORD)[3],appName.split(NGINX_ACCESS_KEYWORD)[0].split(SYSTEM_SUM_SEPARATOR)[1],appName.split(NGINX_ACCESS_KEYWORD)[0].split(SYSTEM_SUM_SEPARATOR)[0],appName.split(NGINX_ACCESS_KEYWORD)[1],appName.split(NGINX_ACCESS_KEYWORD)[2],val,});
                   // insertParams.add(new Object[]{appName.split(NGINX_ACCESS_KEYWORD)[3],appName.split(NGINX_ACCESS_KEYWORD)[1],appName.split(NGINX_ACCESS_KEYWORD)[1],appName.split(NGINX_ACCESS_KEYWORD)[1],appName.split(NGINX_ACCESS_KEYWORD)[2],val,});
                }else{
                    insertParams.add(new Object[]{appName.split(NGINX_ACCESS_KEYWORD)[3],appName.split(NGINX_ACCESS_KEYWORD)[0],appName.split(NGINX_ACCESS_KEYWORD)[0],appName.split(NGINX_ACCESS_KEYWORD)[1],appName.split(NGINX_ACCESS_KEYWORD)[2],val,});
                }
               // insertParams.add(new Object[]{appName.split(NGINX_ACCESS_KEYWORD)[3],appName.split(NGINX_ACCESS_KEYWORD)[0].split(SYSTEM_SUM_SEPARATOR)[1],appName.split(NGINX_ACCESS_KEYWORD)[0].split(SYSTEM_SUM_SEPARATOR)[0],appName.split(NGINX_ACCESS_KEYWORD)[1],appName.split(NGINX_ACCESS_KEYWORD)[2],val,});
               // System.out.println("+++++++++++++++"+appName +"val " +val );
            }
        }
        String insertSQL = "insert into fact_log_sum_nginx_access values(?,?,?,?,?,?)";
        String  updateSQL = "update  fact_log_sum_nginx_access set log_count=? where server_name=? and atdate=? and status_code=?";
        jdbc.doBatch(insertSQL,insertParams);
        jdbc.doBatch(updateSQL,updateParams);
    }


    private static void doPhpError(Map<String, Integer> newlogs, String date) throws Exception {
        String sql ="select * from fact_log_php_sum_platform where atdate > '"+date+"'";
        JDBCUtil jdbc = JDBCUtil.getJDBCInstance();
        final Map<String ,Integer> logs = new HashMap<String,Integer>();
        List<Object[]> insertParams = new ArrayList<Object[]>();
        List<Object[]> updateParams = new ArrayList<Object[]>();
        jdbc.doQuery(sql, null, new ExecuteCallBack() {
            @Override
            public void resultCallBack(ResultSet result) throws Exception {
                while(result.next()){
                    String  platform_name = result.getString("platform_name");
                    String  atdate = result.getString("atdate");
                    String  collation_name = result.getString("collation_name");
                    int count = result.getInt("error_log_count");
                    logs.put(collation_name+PHP_ERROR_KEYWORD+platform_name+PHP_ERROR_KEYWORD+atdate,count);
                    // System.out.println("+++++++++++++++"+word+"#!#"+recordTime);
                }
            }
        });
        for ( Map.Entry<String, Integer> entry : newlogs.entrySet()){
            String appName  = entry.getKey();
            if (appName == null){
                continue;
            }
            Integer val = entry.getValue();
            Integer oldVal =logs.get(appName);
            if(oldVal != null && appName.split(PHP_ERROR_KEYWORD ).length == 3){
                updateParams.add(new Object[]{oldVal+val,appName.split(PHP_ERROR_KEYWORD)[0],appName.split(PHP_ERROR_KEYWORD)[1]});
            }else if (appName.split(PHP_ERROR_KEYWORD ).length == 3){
                insertParams.add(new Object[]{appName.split(PHP_ERROR_KEYWORD)[2],appName.split(PHP_ERROR_KEYWORD)[0],appName.split(PHP_ERROR_KEYWORD)[1],val,});
            }
        }
        String insertSQL = "insert into fact_log_php_sum_platform values(?,?,?,?)";
        String  updateSQL = "update  fact_log_php_sum_platform set error_log_count=? where platform_name=? and atdate=?";
        jdbc.doBatch(insertSQL,insertParams);
        jdbc.doBatch(updateSQL,updateParams);
    }


}

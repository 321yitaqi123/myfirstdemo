package com.beenbank;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

public class MllibTest {
    public static void main(String[] args) {

        //本地运行设置
       System.setProperty("hadoop.home.dir", "C:\\Users\\admin\\Desktop\\work\\hadoop-2.7.5");
       SparkConf conf = new SparkConf().setAppName("ssss").setMaster("local[2]");
        //yarn环境设置
        //SparkConf conf = new SparkConf().setAppName(STREAMING_APPNAME);
        JavaSparkContext jsc =new JavaSparkContext(conf) ;
       JavaRDD<String> data= jsc.textFile("data.txt");

        JavaRDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String line) throws Exception {
                String[] parts = line.split("\t");
                double[] v = new double[parts.length-1];
                for (int i = 0; i < v.length; i++) {
                    v[i] = Double.parseDouble(parts[i+1]);
                }
                return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));  } });
        JavaRDD<LabeledPoint>[] javaRDDS = parsedData.randomSplit(new double[]{0.7, 0.3});
        RDD<LabeledPoint> trainData = javaRDDS[0].rdd();
        RDD<LabeledPoint> testData = javaRDDS[1].rdd();
         LogisticRegressionModel trainFGS = new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainData);

        System.out.println(trainFGS.toString());
        System.out.println(trainFGS.weights());


    }
}
package com.word;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.lang.Object;
import java.util.Arrays;

public class WordC{
    public static void main(String[] args) {
        SparkConf SparkConf = new SparkConf().setMaster("local").setAppName("Java WordCount");
        JavaSparkContext sparkContext = new JavaSparkContext(SparkConf);
        //JavaRDD<String> input = sparkContext.textFile("F:/words.txt");
        JavaRDD<String> input = sparkContext.textFile(args[0]);
        JavaRDD<String> fmap = input.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
        JavaPairRDD ma = fmap.mapToPair(m -> new Tuple2(m,1))
                .reduceByKey(( x, y) ->  "x + y");
        //ma.saveAsTextFile("F:/Java_wordcount_ou");
        ma.saveAsTextFile(args[1]);
        //System.out.println(ma.collect());




    }
}


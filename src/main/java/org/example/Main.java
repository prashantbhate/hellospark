package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        String appName = "hello-spark";
        String master = "local";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
            JavaRDD<Integer> distData = sc.parallelize(data);
            JavaRDD<Integer> map = distData.map(a->a);
            int sum = map.reduce((a,b)->a+b);
            System.out.printf("sum:%d%n", sum);

            JavaRDD<String> lines = sc.textFile("data.txt");
            JavaRDD<Integer> lineLengths = lines.map(String::length);
            int totalLength = lineLengths.reduce(Integer::sum);
            System.out.printf("totalLength:%d%n", totalLength);


            printResultAndCount(lines);
            JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\W+")).iterator());
            printResultAndCount(words);
            JavaRDD<String> nonEmptyWords=words.filter(word -> !word.isBlank());
            printResultAndCount(nonEmptyWords);

            JavaPairRDD<String, Integer> pairs = nonEmptyWords.mapToPair(s -> new Tuple2<>(s, 1));
            printResultAndCount(pairs);

            JavaPairRDD<String, Integer> counts = pairs.reduceByKey(Integer::sum);


            printResultAndCount(counts);

            printResultAndCount(counts.sortByKey());

            JavaPairRDD<Integer, String> swappedPairs = counts
                    .mapToPair(pair -> new Tuple2<>(pair._2, pair._1));

            printResultAndCount(swappedPairs);
            JavaPairRDD<Integer,String> sorted = swappedPairs.sortByKey(false);

            printResultAndCount(sorted);
            List<Tuple2<Integer,String>> collect = sorted.take(5);
            for (Tuple2<Integer,String> t : collect) {
                System.out.printf("(%s,%s)\n",t._2(),t._1());
            }

            Thread.sleep(1000000L);

        }

    }

    private static <T> void printResultAndCount(JavaRDD<T> lines) {
        System.out.printf("count:%d, Result:%s %n",  lines.count(),lines.collect());
    }
    private static <T1,T2> void printResultAndCount(JavaPairRDD<T1,T2>  lines) {
        System.out.printf("count:%d, Result:%s %n",  lines.count(),lines.collect());
    }

}
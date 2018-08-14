package ir.nimbo.pagerank;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.*;

public class RankCalculatorTest {

    @Test
    public void calculate() {
    }

    @Test
    public void getFromHBase() {
    }

    @Test
    public void getResult() {
//        HashSet<String> outLinkSet = new HashSet<>();
//        String url ="";
////        RankCalculator calculator = new RankCalculator("Test", "spark://master-node:9200");
//        RankCalculator calculator = new RankCalculator();
//        List <Tuple2<String,HashSet<String>>> list = new ArrayList<>();
//        SparkConf sc = new SparkConf().setAppName("test").setMaster("spark://master-node:9200");
//        list.add(new Tuple2<>(url,outLinkSet));
//        JavaSparkContext sparkContext = new JavaSparkContext(sc);
//        JavaPairRDD<String, HashSet<String>> input = sparkContext.parallelizePairs(list);
//        calculator.getResult(input);
    }
}
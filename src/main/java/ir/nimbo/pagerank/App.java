package ir.nimbo.pagerank;

import org.apache.spark.api.java.JavaPairRDD;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
//        RankCalculator calculator = new RankCalculator("Testing", "spark://master-node:7077");
//        System.out.println("before collection");
//        calculator.calculate();
//        System.out.println("after collection");
        new RankCalculator("PageRank", "spark://master-node:7077").calculate();
    }
}

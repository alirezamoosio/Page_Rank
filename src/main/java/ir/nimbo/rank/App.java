package ir.nimbo.rank;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        new RankCalculator("Rank", "spark://master-node:7077").calculate();
    }
}

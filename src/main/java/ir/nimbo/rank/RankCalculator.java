package ir.nimbo.rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RankCalculator {
    private static final double DAMPING_FACTOR = 0.85;
    private static final double ITERATION_NUMBER = 50;
    private static String familyName = "context";
    private static String outLinksName = "outLinks";
    private static String pageRankName = "pageRank";
    private Configuration hbaseConf;
    private JavaSparkContext sparkContext;
    private static Logger logger = Logger.getLogger("error");

    RankCalculator(String appName, String master) {
        String[] jars = {"/home/rank/target/Page_Rank-1.0-SNAPSHOT-jar-with-dependencies.jar"};
        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master).setJars(jars);
        sparkContext = new JavaSparkContext(sparkConf);
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.addResource(getClass().getResource("/hbase-site.xml"));
        hbaseConf.addResource(getClass().getResource("/core-site.xml"));
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "webpage");
        hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "context");
    }


    public void calculate() {
        JavaPairRDD<String, ArrayList<Link>> input = getFromHBase();
        JavaPairRDD<String, Integer> result = getResult(input);
        writeToHBase(result);
    }


    JavaPairRDD<String, ArrayList<Link>> getFromHBase() {
        JavaPairRDD<ImmutableBytesWritable, Result> read = sparkContext.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        return read.mapToPair(pair -> {
            String key = Bytes.toString(pair._1.get());
            byte[] outLinksByte = pair._2.getColumnLatestCell(familyName.getBytes(), outLinksName.getBytes()).getValue();
            ArrayList<Link> outLinks = (ArrayList<Link>) SerializationUtils.deserialize(outLinksByte);
            return new Tuple2<>(key, outLinks);
        });
    }

    JavaPairRDD<String, Integer> getResult(JavaPairRDD<String, ArrayList<Link>> input) {
        JavaPairRDD<String, Integer> mapped;
        JavaPairRDD<String, Integer> result;
        mapped = input.flatMapToPair(key -> {
            List<Tuple2<String, Integer>> resultList = new ArrayList<>();
//            System.out.println(key._1);
            try {
                if (key._2 != null) {
                    if (!key._2.isEmpty())
                        for (Link link : key._2) {
                            resultList.add(new Tuple2<>(link.getUrl(), 1));
                        }
                }
            } catch (NullPointerException e) {
//                System.out.println(key._1);
            }
            return resultList;
        });

        result = mapped.reduceByKey((value1, value2) -> value1 + value2);
        return result;
    }

    void writeToHBase(JavaPairRDD<String, Integer> toWrite) {
        try {
            Job jobConfig = Job.getInstance(hbaseConf);
            // TODO: 8/11/18 replace test with webpage
            jobConfig.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "test");
            jobConfig.setOutputFormatClass(TableOutputFormat.class);
            JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = toWrite.mapToPair(pair -> {
                Put put = new Put(Bytes.toBytes(pair._1));
                put.addColumn(familyName.getBytes(), pageRankName.getBytes(), Bytes.toBytes(pair._2));
                return new Tuple2<>(new ImmutableBytesWritable(), put);
            });
            hbasePuts.saveAsNewAPIHadoopDataset(jobConfig.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

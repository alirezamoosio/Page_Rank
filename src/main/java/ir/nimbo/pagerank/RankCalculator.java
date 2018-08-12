package ir.nimbo.pagerank;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RankCalculator {
    private static final double DAMPING_FACTOR = 0.85;
    private static final double ITERATION_NUMBER = 50;
    private static String familyName = "context";
    private static String outLinksName = "outLinks";
    private static String pageRankName = "pageRank";
    private Configuration hbaseConf;
    private JavaSparkContext sparkContext;

    RankCalculator(String appName, String master) {
        String[] jars = {"/home/alireza/Page_Rank/target/Page_Rank-1.0-SNAPSHOT-jar-with-dependencies.jar"};
        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master).setJars(jars);
        sparkContext = new JavaSparkContext(sparkConf);
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.addResource(getClass().getResource("/hbase-site.xml"));
        hbaseConf.addResource(getClass().getResource("/core-site.xml"));
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "webpage");
        hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "context");
    }

    public void calculate() {
        JavaPairRDD<String, Value> input = getFromHBase();
        JavaPairRDD<String, Value> result = getResult(input);
        writeToHBase(result);
    }

    JavaPairRDD<String, Value> getFromHBase() {
        JavaPairRDD<ImmutableBytesWritable, Result> read =
                sparkContext.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        return read.mapToPair(pair -> {
            String key = Bytes.toString(pair._1.get());
            String serializedMap = Bytes.toString(pair._2.getColumnLatestCell(familyName.getBytes(), outLinksName.getBytes()).getValue());
            Double pageRank = Bytes.toDouble(pair._2.getColumnLatestCell(familyName.getBytes(), pageRankName.getBytes()).getValue());
            Gson gson = new Gson();
            if (serializedMap.contains("www.livejournal.com"))
                return null;
            try {
                Map<String, String> outLinks = gson.fromJson(serializedMap, Map.class);
                return new Tuple2<>(key, new Value(outLinks.keySet(), pageRank));
            } catch (JsonSyntaxException e) {
                e.printStackTrace();
                System.out.println(serializedMap);
                return null;
            }
        });
    }

    public JavaPairRDD<String, String> testRead() {
        JavaPairRDD<ImmutableBytesWritable, Result> read =
                sparkContext.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        return read.mapToPair(pair -> {
            String key = Bytes.toString(pair._1.get());
            String value = Bytes.toString(pair._2.getColumnLatestCell("context".getBytes(), "f1".getBytes()).getValue());
            return new Tuple2<>(key, value);
        });
    }

    public void testWrite(JavaPairRDD<String, String> toWrite) {

        JavaPairRDD<ImmutableBytesWritable, Put> toPut = toWrite.mapToPair(input -> {
            Put put = new Put(input._1.getBytes());
            put.addColumn("context".getBytes(), "f1".getBytes(), ("new" + input._2).getBytes());
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });
        try {
            Job job = Job.getInstance(hbaseConf);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "test");
            job.setOutputFormatClass(TableOutputFormat.class);
            toPut.saveAsNewAPIHadoopDataset(job.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private JavaPairRDD<String, Value> getResult(JavaPairRDD<String, Value> input) {
        JavaPairRDD<String, Value> mapped;
        for (int i = 0; i < ITERATION_NUMBER; i++) {
            mapped = input.flatMapToPair(key -> {
                List<Tuple2<String, Value>> result = new ArrayList<>();
                for (String url : key._2.outLinks) {
                    result.add(new Tuple2<>(url, new Value(null, DAMPING_FACTOR * key._2.pageRank / key._2.outLinks.size())));
                }
                result.add(new Tuple2<>(key._1, new Value(key._2.outLinks, 1 - DAMPING_FACTOR)));
                return result.iterator();
            });
            input = mapped.reduceByKey((value1, value2) -> {
                Set<String> finalList = value1.outLinks == null ? value2.outLinks : value1.outLinks;
                double finalRank = value1.pageRank + value2.pageRank;
                return new Value(finalList, finalRank);
            });
        }
        return input;
    }

    void writeToHBase(JavaPairRDD<String, Value> toWrite) {
        try {
            Job jobConfig = Job.getInstance(hbaseConf);
            // TODO: 8/11/18 replace test with webpage
            jobConfig.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "test");
            jobConfig.setOutputFormatClass(TableOutputFormat.class);
            JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = toWrite.mapToPair(pair -> {
                System.out.println(pair._2.pageRank);
                Gson gson = new Gson();
                Put put = new Put(Bytes.toBytes(pair._1));
                put.addColumn(familyName.getBytes(), outLinksName.getBytes(), gson.toJson(pair._2.outLinks).getBytes());
                put.addColumn(familyName.getBytes(), pageRankName.getBytes(), Bytes.toBytes(pair._2.pageRank));
                return new Tuple2<>(new ImmutableBytesWritable(), put);
            });
            hbasePuts.saveAsNewAPIHadoopDataset(jobConfig.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (sparkContext != null)
            sparkContext.close();
    }

}

package com.datamatica.assignment;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Assignment1 {
    public static class Record implements Serializable {
        public Record(String userId, String productId, String category, String action) {
            super();
            this.userId = userId;
            this.productId = productId;
            this.category = category;
            this.action = action;
        }

        private static final long serialVersionUID = 1L;
        private String userId;
        private String productId, category, action;

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Assignment1").master("local[2]").config("spark.some.config.option", "some-value")
                .getOrCreate();

        Encoder<Record> recordEncoder = Encoders.bean(Record.class);
        
        //Load data from text file to DataSet
        Dataset<Record> df = spark.read().textFile("src/main/resources/Assignment1.txt").map(new MapFunction<String, Record>() {

            public Record call(String s) throws Exception {
                // TODO Auto-generated method stub
                if (StringUtils.isEmpty(s)) {
                    return null;
                } else {
                    String[] str = s.split(":");
                    Record r = new Record(str[0], str[1], str[2], str[3]);
                    return r;
                }
            }
            
        }, recordEncoder);
        //Filter only those data having action as click or browse
        Dataset<Row> countDF = df.groupBy("productId", "action").count().orderBy("count").filter("action='click' or action='browse'");
        countDF.show();

        //1.Convert dataset to RDD
        JavaRDD<Tuple2<String, Tuple2<Integer, Integer>>> recordRdd = countDF.toJavaRDD()
                //2. Convert Rdd to pairRDD in format (ProductId, BrowseCount, ClickCount)
                .mapToPair(new PairFunction<Row, String, Tuple2<Integer, Integer>>() {

                    @Override
                    public Tuple2<String, Tuple2<Integer, Integer>> call(Row arg0) throws Exception {
                        if (arg0.getString(1).equalsIgnoreCase("browse")) {
                            return new Tuple2<String, Tuple2<Integer, Integer>>(arg0.getString(0), new Tuple2((int) arg0.getLong(2), 0));
                        } else if (arg0.getString(1).equalsIgnoreCase("click")) {
                            return new Tuple2<String, Tuple2<Integer, Integer>>(arg0.getString(0), new Tuple2(0, (int) arg0.getLong(2)));
                        }
                        return null;
                    }
                })//3. Reduce the data on the basis of key
                .reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> arg0, Tuple2<Integer, Integer> arg1) throws Exception {
                        return new Tuple2<Integer, Integer>(arg0._1 + arg1._1, arg0._2 + arg1._2);
                    }
                })//4. As i have to sort pairRdd on the basis of value. so i am converting pairRdd<K,V> to plain RDD<tuple<k,v>> 
                .map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Tuple2<String, Tuple2<Integer, Integer>>>() {

                    public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Tuple2<Integer, Integer>> arg0) throws Exception {
                        return arg0;
                    }
                })//5. Applying sortby on browse count + click count, in descending order 
                .sortBy(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Integer>() {

                    public Integer call(Tuple2<String, Tuple2<Integer, Integer>> arg0) throws Exception {
                        return arg0._2._1 + arg0._2._2;
                    }
                }, false, 1);
        
        //Take top ten most browsed data
        List<Tuple2<String, Tuple2<Integer, Integer>>> list = recordRdd.take(10);
        System.out.println();
        System.out.println("UserId------Browse------Click");
        list.forEach(obj -> System.out.println(obj._1 + "  " + obj._2()._1 + "  " + obj._2._2));
    }
}

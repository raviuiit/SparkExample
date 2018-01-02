package com.datamatica.assignment;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Assignment2 {
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

        // Load data from text file to DataSet
        Dataset<Record> df = spark.read().textFile("src/main/resources/Assignment2.txt").map(new MapFunction<String, Record>() {

            @Override
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

        Dataset<Row> countDF = df.select("userId", "category", "action").filter("action='click' or action='addtocart' or action='purchase'")
                .filter("category='Food' or category='Kids'").distinct().orderBy("userId");
        countDF.show();
        JavaPairRDD<String, String> kidsdRdd = countDF.toJavaRDD().filter(new Function<Row, Boolean>() {

            @Override
            public Boolean call(Row arg0) throws Exception {
                if (arg0.getString(1).equalsIgnoreCase("Kids")) {
                    return true;
                }
                return false;
            }
        }).mapToPair(new PairFunction<Row, String, String>() {

            @Override
            public Tuple2<String, String> call(Row t) throws Exception {
                // TODO Auto-generated method stub
                return new Tuple2<String, String>(t.getString(0), t.getString(1));
            }
        });
        JavaPairRDD<String, String> foodRdd = countDF.toJavaRDD().filter(new Function<Row, Boolean>() {

            @Override
            public Boolean call(Row arg0) throws Exception {
                if (arg0.getString(1).equalsIgnoreCase("Food")) {
                    return true;
                }
                return false;
            }
        }).mapToPair(new PairFunction<Row, String, String>() {

            @Override
            public Tuple2<String, String> call(Row t) throws Exception {
                return new Tuple2<String, String>(t.getString(0), t.getString(1));
            }
        });
        Map<String, String> interstedUserMap = kidsdRdd.subtractByKey(foodRdd).collectAsMap();
        System.out.println("\n\n ===Interested Users===");
        for(String key: interstedUserMap.keySet()){
            System.out.println(key);
        }
        
        
    }

}

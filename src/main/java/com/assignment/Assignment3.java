package com.assignment;

import java.io.Serializable;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** @author impadmin Problem: we have a text file having 3 fields (name, year, salary) a person can have mutiple entry for salary for same year or for
 *         different year. No two person can have same name We have to write a spark solution for finding top n salary for a particular year based on
 *         user input */
public class Assignment3 {
    public static class Employee implements Serializable {

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }

        public double getSalary() {
            return salary;
        }

        public void setSalary(double salary) {
            this.salary = salary;
        }

        public static long getSerialversionuid() {
            return serialVersionUID;
        }

        public Employee(String name, int year, double salary) {
            super();
            this.name = name;
            this.year = year;
            this.salary = salary;
        }

        private static final long serialVersionUID = 1L;
        private String name;
        private int year;
        private double salary;
        // private double avgSalary;

    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Assignment3").master("local[2]").config("spark.some.config.option", "some-value")
                .getOrCreate();

        Encoder<Employee> recordEncoder = Encoders.bean(Employee.class);
        // Load data from text file to DataSet
        Dataset<Employee> df = spark.read().textFile("src/main/resources/Assignment3.txt").map(new MapFunction<String, Employee>() {

            public Employee call(String s) throws Exception {
                if (StringUtils.isEmpty(s)) {
                    return null;
                } else {
                    String[] str = s.split(",");
                    Employee r = new Employee(str[0], Integer.parseInt(str[1]), Double.parseDouble(str[2]));
                    return r;
                }
            }
        }, recordEncoder);
        // Now calculate avg Salary per year per person basis and sort them on the basis of year
        Dataset<Row> df2005 = df.groupBy("name", "year").avg("salary").as("avgSalary").orderBy(new Column("year").asc(),
                new Column("avg(salary)").desc());
        df2005.printSchema();
        // Print the result
        df2005.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row t) throws Exception {
                System.out.println();
                System.out.print(t);
            }
        });
        // This block will take year and no of record to show from user and show top records of that year.
        Scanner reader = null;
        for (int i = 0; i < 5; i++) {
            reader = new Scanner(System.in);
            System.out.println("Enter the Year for which you want to get the data: ");
            int year = reader.nextInt();
            // once finished
            System.out.println("Enter the numRecord to show:");
            int count = reader.nextInt();

            df2005.filter("year=" + year).orderBy(new Column("avg(salary)").desc()).takeAsList(count).forEach(row -> System.out.println(row));
        }
        reader.close();
    }
}

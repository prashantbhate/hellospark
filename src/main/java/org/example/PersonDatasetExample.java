package org.example;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Encoders;

import java.util.Objects;

import static org.apache.spark.sql.functions.col;

public class PersonDatasetExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Dataset Example")
                .master("local")
                .getOrCreate();

        String jsonFilePath = Objects.requireNonNull(PersonDatasetExample.class.getClassLoader().getResource("people.json")).getPath();;

        processDataset(spark, jsonFilePath);
        processDataframe(spark, jsonFilePath);

    }

    private static void processDataframe(SparkSession spark, String jsonFilePath) {

        System.out.println("processDataframe()");

        Dataset<Row> peopleDataFrame = spark.read()
                .json(jsonFilePath);

        peopleDataFrame.show();

        Dataset<Row> filter = peopleDataFrame.filter("age IS NOT NULL AND age > 20");

        filter.show();

    }

    private static void processDataset(SparkSession spark, String jsonFilePath) {

        System.out.println("processDataset()");

        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        Dataset<Person> peopleDataset = spark.read()
                .json(jsonFilePath)
                .select(
                        col("name"),
                        col("age").cast("int").as("age")
                )
                .as(personEncoder);

        peopleDataset.show();

        Dataset<Person> filteredDataset = peopleDataset.filter((FilterFunction<Person>) person -> person.getAge() != null && person.getAge() > 20);
        filteredDataset.show();
    }

    // Define the Person class
    public static class Person {
        private String name;
        private Integer age; // Integer to allow for null values

        // Getters and setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public Integer getAge() { return age; }
        public void setAge(Integer age) { this.age = age; }
    }
}

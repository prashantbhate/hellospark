package org.example;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.example.Person.mapper;

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

        System.out.println("===============processDataframe()=================");

        Dataset<Row> peopleDataFrame = spark.read()
                .json(jsonFilePath);

        peopleDataFrame.show();

        Dataset<Row> filter = peopleDataFrame.filter("age IS NOT NULL AND age > 20");

        filter.show();

    }

    private static void processDataset(SparkSession spark, String jsonFilePath) {

        System.out.println("=================processDataset()=====================");

        System.out.println("Reading JSON file without any specified schema:");
        Dataset<Row> df = spark.read()
                .json(jsonFilePath);
        df.printSchema();
        df.show();

        System.out.println("Reading JSON file with the specified schema:");
        StructType schema = new StructType()
                .add("name", "string")
                .add("age", "string")  // Assuming age is a string
                .add("hire_date", "string");  // Assuming hire_date is a string

        df = spark.read()
                .schema(schema)
                .json(jsonFilePath);
        df.printSchema();
        df.show();

        System.out.println("Selecting specific columns creating new columns optionally casting some columns:");
        df = df.select(
                col("name"),
                col("age").as("age_str"),
                col("hire_date").as("hire_date_str"),
                col("age").cast("int").as("age"),
                col("hire_date").cast("date").as("hire_date")
        );
        df.printSchema();
        df.show();

        System.out.println("Creating new column 'hire_date_parsed' with parsed date format:");
        df = df.withColumn("hire_date_parsed", to_date(col("hire_date_str"), "MM-yyyy-dd"));
        df.printSchema();
        df.show();

        System.out.println("Selecting only required columns after renaming:");
        df = df.select(
                col("name"),
                col("age_str").as("age"),
                col("hire_date_str").as("hire_date")
        );
        df.printSchema();

        System.out.println("Mapping DataFrame to Dataset of Person objects:");
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> peopleDataset = df.map(mapper, personEncoder);
        peopleDataset.printSchema();
        peopleDataset.show();

        System.out.println("Filtering Dataset where age > 20:");
        Dataset<Person> filteredDataset = peopleDataset.filter((FilterFunction<Person>) person -> person.getAge() != null && person.getAge() > 20);
        filteredDataset.show();

    }


}



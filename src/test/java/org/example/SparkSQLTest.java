package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;

public class SparkSQLTest {

    private static SparkSession sparkSession;

    @BeforeAll
    static void setUp() {
        sparkSession = SparkSession.builder()
                .appName("SparkSQLTest")
                .master("local[8]")
                .getOrCreate();
    }

    @AfterAll
    static void tearDown() {

        sleep();
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        // Get the name of the current test method
        String methodName = testInfo.getDisplayName();
        sparkSession.sparkContext().setJobGroup("SparkSQLTest",methodName,true);
        System.out.println("About to run test method: " + methodName);
    }

    private static void sleep() {
        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    void testCreateDataFrameAndSQLQuery() {
        Dataset<Row> df = sparkSession.createDataFrame(
                List.of(
                        RowFactory.create(1, "Alice", 29),
                        RowFactory.create(2, "Bob", 35)
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType, false)
                        .add("name", DataTypes.StringType, false)
                        .add("age", DataTypes.IntegerType, false)
        );

        df.explain();

        df.createOrReplaceTempView("people");

        Dataset<Row> result = sparkSession.sql("SELECT * FROM people WHERE age > 30");

        result.explain();

        assertEquals(1, result.count());
        assertEquals("Bob", result.collectAsList().get(0).getString(1));
    }

    @Test
    void testAggregation() {
        ClassLoader classLoader = getClass().getClassLoader();
        String filePath = Objects.requireNonNull(classLoader.getResource("people.json")).getPath();
        Dataset<Row> df = sparkSession.read().json(filePath);

        df.createOrReplaceTempView("people");

        Dataset<Row> result = sparkSession.sql("SELECT AVG(age) as avg_age FROM people");

        double avgAge = result.collectAsList().get(0).getDouble(0);
        assertTrue(avgAge > 0);
    }

    @Test
    void testJoinOperations1() {
        Dataset<Row> df1 = sparkSession.createDataFrame(
                List.of(
                        RowFactory.create(1, "Alice"),
                        RowFactory.create(2, "Bob")
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType, false)
                        .add("name", DataTypes.StringType, false)
        );

        Dataset<Row> df2 = sparkSession.createDataFrame(
                List.of(
                        RowFactory.create(1, "HR"),
                        RowFactory.create(2, "Finance")
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType, false)
                        .add("department", DataTypes.StringType, false)
        );

        Dataset<Row> joined = df1.join(df2, "id");

        assertEquals(2, joined.count());
        assertEquals(3, joined.columns().length);
    }

    @Test
    void testWindowFunctions1() {
        Dataset<Row> df = sparkSession.createDataFrame(
                List.of(
                        RowFactory.create("HR", "Alice", 1000),
                        RowFactory.create("HR", "Bob", 1500),
                        RowFactory.create("Finance", "Charlie", 2000)
                ),
                new StructType()
                        .add("department", DataTypes.StringType, false)
                        .add("name", DataTypes.StringType, false)
                        .add("salary", DataTypes.IntegerType, false)
        );

        df.createOrReplaceTempView("employees");

        Dataset<Row> result = sparkSession.sql("""
            SELECT department, name, salary,
                   RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank
            FROM employees
        """);

        assertEquals(3, result.count());
        assertEquals(1, result.collectAsList().get(0).getInt(3)); // Alice's rank in HR
    }


    @Test
    public void testDataFrameCreation() {
        // Creating DataFrame from a list of Rows
        List<Row> data = Arrays.asList(
                RowFactory.create(1, "Alice", 25),
                RowFactory.create(2, "Bob", 30),
                RowFactory.create(3, "Charlie", 35)
        );

        // Define schema
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false)
        ));

        // Create DataFrame
        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        // Assertions
        assertEquals(3, df.count());
        assertEquals(3, df.columns().length);
    }

    @Test
    public void testDataFrameOperations() {
        // Create sample DataFrame
        Dataset<Row> df = sparkSession.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice", 25),
                RowFactory.create(2, "Bob", 30),
                RowFactory.create(3, "Charlie", 35)
        ), DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false)
        )));

        // Select operation
        Dataset<Row> selectedDf = df.select("name", "age");
        assertEquals(2, selectedDf.columns().length);

        // Filter operation
        Dataset<Row> filteredDf = df.filter(col("age").gt(27));
        assertEquals(2, filteredDf.count());

        // Aggregate operations
        long count = df.count();
        assertEquals(3, count);

        // Group by and aggregate
        Dataset<Row> groupedDf = df.groupBy("age")
                .agg(
                        functions.count("name").alias("name_count"),
                        functions.max("id").alias("max_id")
                );
        assertEquals(3, groupedDf.count());
    }

    @Test
    public void testWindowFunctions() {
        // Create sample data for window function testing
        Dataset<Row> df = sparkSession.createDataFrame(Arrays.asList(
                RowFactory.create("Dept1", "Alice", 5000),
                RowFactory.create("Dept1", "Bob", 6000),
                RowFactory.create("Dept2", "Charlie", 4500),
                RowFactory.create("Dept2", "David", 7000)
        ), DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("department", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("salary", DataTypes.IntegerType, false)
        )));

        // Define window specification
        WindowSpec windowSpec = Window.partitionBy("department")
                .orderBy(col("salary").desc());

        // Apply window functions
        Dataset<Row> windowedDf = df.withColumn("rank",
                functions.rank().over(windowSpec)
        ).withColumn("dense_rank",
                functions.dense_rank().over(windowSpec)
        );

        // Demonstrate window function results
        List<Row> results = windowedDf.collectAsList();
        assertNotNull(results);
        assertEquals(4, results.size());
    }

    @Test
    public void testJoinOperations() {
        // Create first DataFrame
        Dataset<Row> employees = sparkSession.createDataFrame(Arrays.asList(
                RowFactory.create(1, "Alice", 101),
                RowFactory.create(2, "Bob", 102),
                RowFactory.create(3, "Charlie", 103)
        ), DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("emp_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("dept_id", DataTypes.IntegerType, false)
        )));

        // Create second DataFrame
        Dataset<Row> departments = sparkSession.createDataFrame(Arrays.asList(
                RowFactory.create(101, "HR"),
                RowFactory.create(102, "Finance"),
                RowFactory.create(103, "IT")
        ), DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("dept_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("dept_name", DataTypes.StringType, false)
        )));

        // Inner Join
        Dataset<Row> innerJoin = employees.join(departments,
                employees.col("dept_id").equalTo(departments.col("dept_id")),
                "inner"
        );
        assertEquals(3, innerJoin.count());

        // Left Outer Join
        Dataset<Row> leftOuterJoin = employees.join(departments,
                employees.col("dept_id").equalTo(departments.col("dept_id")),
                "left_outer"
        );
        assertEquals(3, leftOuterJoin.count());

        // Right Outer Join
        Dataset<Row> rightOuterJoin = employees.join(departments,
                employees.col("dept_id").equalTo(departments.col("dept_id")),
                "right_outer"
        );
        assertEquals(3, rightOuterJoin.count());
    }

    @Test
    public void testUDFs() {
        // Register a User Defined Function (UDF)
        sparkSession.udf().register("square", (Integer x) -> x * x, DataTypes.IntegerType);

        // Create a sample DataFrame
        Dataset<Row> df = sparkSession.createDataFrame(Arrays.asList(
                RowFactory.create(1),
                RowFactory.create(2),
                RowFactory.create(3)
        ), DataTypes.createStructType(List.of(
                DataTypes.createStructField("number", DataTypes.IntegerType, false)
        )));

        // Apply UDF
        Dataset<Row> resultDf = df.withColumn("squared",
                functions.callUDF("square", col("number"))
        );

        // Collect and verify results
        List<Row> results = resultDf.collectAsList();
        assertEquals(1, results.get(0).getInt(1));   // 1^2 = 1
        assertEquals(4, results.get(1).getInt(1));   // 2^2 = 4
        assertEquals(9, results.get(2).getInt(1));   // 3^2 = 9
    }

    @Test
    public void testComplexDataTypes() {
        // Create DataFrame with Array and Map columns
        Dataset<Row> df = sparkSession.createDataFrame(Arrays.asList(
                RowFactory.create(
                        Arrays.asList(1, 2, 3),
                        Collections.singletonMap("key1", "value1")
                ),
                RowFactory.create(
                        Arrays.asList(4, 5, 6),
                        Collections.singletonMap("key2", "value2")
                )
        ), DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("numbers",
                        DataTypes.createArrayType(DataTypes.IntegerType), false),
                DataTypes.createStructField("mapping",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false)
        )));

        // Operations on complex types
        Dataset<Row> processedDf = df.withColumn("array_size",
                functions.size(col("numbers"))
        ).withColumn("map_keys",
                functions.map_keys(col("mapping"))
        );

        // Collect and verify results
        List<Row> results = processedDf.collectAsList();
        int arraySize = results.get(0).getAs("array_size");
        assertEquals(3, arraySize);
        assertEquals(1, ((Seq<?>) results.get(0).getAs("map_keys")).size());
    }

    @Test
    public void testAggregationAndAnalytics() {
        // Create sample sales data
        Dataset<Row> salesDf = sparkSession.createDataFrame(Arrays.asList(
                RowFactory.create("Product A", 100.0, "2023-01-15"),
                RowFactory.create("Product B", 200.0, "2023-01-16"),
                RowFactory.create("Product A", 150.0, "2023-01-17"),
                RowFactory.create("Product B", 250.0, "2023-01-18")
        ), DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product", DataTypes.StringType, false),
                DataTypes.createStructField("sales", DataTypes.DoubleType, false),
                DataTypes.createStructField("date", DataTypes.StringType, false)
        )));

        // Perform various aggregations
        Dataset<Row> aggregatedDf = salesDf
                .groupBy("product")
                .agg(
                        functions.sum("sales").alias("total_sales"),
                        functions.avg("sales").alias("avg_sales"),
                        functions.max("sales").alias("max_sales"),
                        functions.min("sales").alias("min_sales")
                );

        // Collect and verify results
        List<Row> results = aggregatedDf.collectAsList();
        assertEquals(2, results.size());

        // Check aggregation results
        for (Row result : results) {
            String product = result.getString(0);
            double totalSales = result.getDouble(1);

            if (product.equals("Product A")) {
                assertEquals(250.0, totalSales, 0.001);
            } else if (product.equals("Product B")) {
                assertEquals(450.0, totalSales, 0.001);
            }
        }
    }

}

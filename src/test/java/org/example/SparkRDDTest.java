package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;

public class SparkRDDTest {

    private static JavaSparkContext sc;

    @BeforeAll
    public static void setUp() {
        // Set up the Spark context
        SparkConf conf = new SparkConf().setMaster("local[8]").setAppName("RDDTest");
        sc = new JavaSparkContext(conf);
    }

    @AfterAll
    public static void tearDown() {

        sleep();
        // Stop the Spark context
        if (sc != null) {
            sc.stop();
        }
    }
    @AfterEach
    public void afterEach(){
//        sleep();
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        // Get the name of the current test method
        String methodName = testInfo.getDisplayName();
        sc.setJobGroup("RDDTest",methodName);
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
    public void testCount() {
        List<Integer> data = asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        // Test the RDD creation
        assertNotNull(rdd);
        assertEquals(5, rdd.count());
    }

    @Test
    public void testMapOperation() {
        List<Integer> data = asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        // Map each element to its square
        JavaRDD<Integer> squaredRDD = rdd.map(x -> x * x);

        // Collect the results and test
        List<Integer> results = squaredRDD.collect();
        assertEquals(asList(1, 4, 9, 16, 25), results);
    }

    @Test
    public void testFilterOperation() {
        List<Integer> data = asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        // Filter even numbers
        JavaRDD<Integer> evenRDD = rdd.filter(x -> x % 2 == 0);

        // Collect the results and test
        List<Integer> results = evenRDD.collect();
        assertEquals(asList(2, 4), results);
    }

    @Test
    public void testFlatMapOperation() {
        List<String> data = asList("hello world", "apache spark");
        JavaRDD<String> rdd = sc.parallelize(data);

        // Split lines into words
        JavaRDD<String> wordsRDD = rdd.flatMap(line -> asList(line.split(" ")).iterator());

        // Collect and test
        List<String> results = wordsRDD.collect();
        assertEqlAnyOrder(asList("hello", "world", "apache", "spark"), results);
    }

    @Test
    public void testReduceOperation() {
        List<Integer> data = asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        // Compute sum of elements
        int sum = rdd.reduce(Integer::sum);

        assertEquals(15, sum);
    }

    @Test
    public void testGroupByKeyOperation() {
        List<Tuple2<String, Integer>> data = asList(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 2),
                new Tuple2<>("A", 3)
        );
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);

        // Group by key
        JavaPairRDD<String, Iterable<Integer>> groupedRDD = pairRDD.groupByKey();
        List<Tuple2<String, Iterable<Integer>>> results = groupedRDD.collect();

        assertEquals(2, results.size());
    }


    @Test
    public void testDistinctOperation() {
        List<Integer> data = asList(1, 2, 2, 3, 3, 3, 4);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        // Remove duplicates
        JavaRDD<Integer> distinctRDD = rdd.distinct();

        List<Integer> results = distinctRDD.collect();
        assertThat(results, containsInAnyOrder(1, 2, 3, 4));
    }

    @Test
    public void testUnionOperation() {
        JavaRDD<Integer> rdd1 = sc.parallelize(asList(1, 2, 3));
        JavaRDD<Integer> rdd2 = sc.parallelize(asList(4, 5, 6));

        // Union two RDDs
        JavaRDD<Integer> unionRDD = rdd1.union(rdd2);

        List<Integer> results = unionRDD.collect();
        assertEquals(asList(1, 2, 3, 4, 5, 6), results);
    }


    @Test
    public void testIntersectionOperation() {
        JavaRDD<Integer> rdd1 = sc.parallelize(asList(1, 2, 3, 4));
        JavaRDD<Integer> rdd2 = sc.parallelize(asList(3, 4, 5, 6));

        // Find intersection
        JavaRDD<Integer> intersectionRDD = rdd1.intersection(rdd2);

        List<Integer> results = intersectionRDD.collect();
        assertEqlAnyOrder(results, List.of(3, 4));
    }

    private static <T> void assertEqlAnyOrder(List<T> expected, List<T> actual) {
        assertThat(actual, containsInAnyOrder(expected.toArray()));
    }

    @Test
    public void testCartesianOperation() {
        JavaRDD<Integer> rdd1 = sc.parallelize(asList(1, 2));
        JavaRDD<String> rdd2 = sc.parallelize(asList("A", "B"));

        // Compute Cartesian product
        JavaPairRDD<Integer, String> cartesianRDD = rdd1.cartesian(rdd2);

        List<Tuple2<Integer, String>> results = cartesianRDD.collect();
        assertEquals(4, results.size());
        assertTrue(results.contains(new Tuple2<>(1, "A")));
        assertTrue(results.contains(new Tuple2<>(1, "B")));
        assertTrue(results.contains(new Tuple2<>(2, "A")));
        assertTrue(results.contains(new Tuple2<>(2, "B")));
    }

    @Test
    public void testSampleOperation() {
        JavaRDD<Integer> rdd = sc.parallelize(asList(1, 2, 3, 4, 5));

        // Sample 50% of the data
        JavaRDD<Integer> sampledRDD = rdd.sample(false, 0.5);

        long sampledCount = sampledRDD.count();
        long unSampledCount = sampledRDD.count();


        String msg = " %d == %d".formatted(sampledCount, sampledCount);
        assertTrue(sampledCount >= 0 && sampledCount <= unSampledCount , msg);

    }

    @Test
    public void testPartitioning() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 3);

        // Verify the number of partitions
        assertEquals(3, rdd.getNumPartitions());

        // Custom partitioning using PairRDD
        List<Tuple2<Integer, String>> data = Arrays.asList(
                new Tuple2<>(1, "A"),
                new Tuple2<>(2, "B"),
                new Tuple2<>(3, "C")
        );
        JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(data).partitionBy(new org.apache.spark.HashPartitioner(2));

        assertEquals(2, pairRDD.getNumPartitions());
    }

    @Test
    public void testRepartitionOperation() {
        JavaRDD<Integer> rdd = sc.parallelize(asList(1, 2, 3, 4, 5), 2);

        // Increase partitions
        JavaRDD<Integer> repartitionedRDD = rdd.repartition(4);

        assertEquals(4, repartitionedRDD.getNumPartitions());
    }

    @Test
    public void testCoalesceOperation() {
        JavaRDD<Integer> rdd = sc.parallelize(asList(1, 2, 3, 4, 5), 4);

        // Reduce partitions
        JavaRDD<Integer> coalescedRDD = rdd.coalesce(2);

        assertEquals(2, coalescedRDD.getNumPartitions());
    }

    @Test
    public void testBroadcastVariable() {
        // Create a broadcast variable with a multiplier
        Broadcast<Integer> multiplier = sc.broadcast(2);

        List<Integer> data = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        // Multiply each element in the RDD by the broadcasted value
        JavaRDD<Integer> multipliedRDD = rdd.map(x -> x * multiplier.value());

        // Collect and test
        List<Integer> results = multipliedRDD.collect();
        assertEquals(Arrays.asList(2, 4, 6, 8), results);
    }


    @Test
    public void testIncorrectWayOfMapOperation() {
        List<Integer> data = asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data);
        final int[] counter = {0};
        // int c=0;

        // Wrong: Don't do this!!
        rdd.foreach(x -> {
            //c++; //Variable used in lambda expression should be final or effectively final
            counter[0] += x;
        });
        int sum = rdd.reduce(Integer::sum);
        assertNotEquals(sum, counter[0]);
    }
    @Test
    public void testAccumulator() {
        // Create an accumulator for summing numbers
        var sumAccumulator = sc.sc().longAccumulator();

        List<Long> data = Arrays.asList(1L, 2L, 3L, 4L);
        JavaRDD<Long> rdd = sc.parallelize(data);

        // Perform an action to add to the accumulator
        rdd.foreach(sumAccumulator::add);

        // Verify the accumulator value
        assertEquals(10L,  sumAccumulator.value());
        assertEquals(2.5,  sumAccumulator.avg());
    }

    @Test
    public void testCachingAndPersistence() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // Verify storage
        assertFalse(rdd.getStorageLevel().useMemory());
        assertFalse(rdd.getStorageLevel().useDisk());

        rdd.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
        rdd.count(); // Action to trigger persistence

        // Verify persistence
        assertTrue(rdd.getStorageLevel().useMemory());
        assertTrue(rdd.getStorageLevel().useDisk());
    }

    @Test
    public void testCaching() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));


        // Verify storage
        assertFalse(rdd.getStorageLevel().useMemory());
        assertFalse(rdd.getStorageLevel().useDisk());


        // Cache the RDD
        rdd.cache();
        rdd.count(); // Action to trigger caching

        // Verify caching
        assertTrue(rdd.getStorageLevel().useMemory());
        assertFalse(rdd.getStorageLevel().useDisk());

    }

    @Test
    public void testCheckpointing() {
        sc.setCheckpointDir("/tmp/spark-checkpoint");

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // Set checkpoint and perform action to trigger it
        rdd.checkpoint();
        rdd.count();

        // Verify checkpointing
        assertTrue(rdd.isCheckpointed());
        assertNotNull(rdd.getCheckpointFile());
    }

    @Test
    public void testTakeActions() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(5, 2, 8, 1, 3));

        // Take the first 3 elements
        List<Integer> taken = rdd.take(3);
        assertEquals(Arrays.asList(5, 2, 8), taken);

        // Take the smallest 3 elements
        List<Integer> smallest = rdd.takeOrdered(3);
        assertEquals(Arrays.asList(1, 2, 3), smallest);

        // Take a sample of elements
        List<Integer> sample = rdd.takeSample(false, 2);
        assertEquals(2, sample.size());
    }

    @Test
    public void testSaveActions(@TempDir Path tempDir) throws IOException {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("apple", "banana", "cherry"));

        // Save as text file (use a temporary directory for testing)
        String filePath = tempDir.toString();
        System.out.println("File:"+filePath);
        Files.delete(tempDir);
        rdd.saveAsTextFile(filePath);

        // Verify that the output file exists (basic test, implementation dependent)
        java.io.File outputDir = new java.io.File(filePath);
        assertTrue(outputDir.exists() && outputDir.isDirectory());
    }



    @Test
    public void testSortByAndSortByKey() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(5, 2, 8, 1, 3));

        // SortBy operation
        JavaRDD<Integer> sortedRDD = rdd.sortBy(x -> x, true, 1);
        List<Integer> results = sortedRDD.collect();
        assertEquals(Arrays.asList(1, 2, 3, 5, 8), results);

        // SortByKey operation
        List<Tuple2<Integer, String>> pairData = Arrays.asList(
                new Tuple2<>(3, "C"),
                new Tuple2<>(1, "A"),
                new Tuple2<>(2, "B")
        );
        JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(pairData);
        JavaPairRDD<Integer, String> sortedByKeyRDD = pairRDD.sortByKey();

        List<Tuple2<Integer, String>> sortedResults = sortedByKeyRDD.collect();
        assertEquals(Arrays.asList(
                new Tuple2<>(1, "A"),
                new Tuple2<>(2, "B"),
                new Tuple2<>(3, "C")
        ), sortedResults);
    }

    @Test
    public void testJoinOperations() {
        List<Tuple2<Integer, String>> rdd1Data = Arrays.asList(
                new Tuple2<>(1, "A"),
                new Tuple2<>(2, "B"),
                new Tuple2<>(3, "C")
        );
        List<Tuple2<Integer, String>> rdd2Data = Arrays.asList(
                new Tuple2<>(1, "X"),
                new Tuple2<>(2, "Y"),
                new Tuple2<>(4, "Z")
        );

        JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(rdd1Data);
        JavaPairRDD<Integer, String> rdd2 = sc.parallelizePairs(rdd2Data);

        // Inner Join
        JavaPairRDD<Integer, Tuple2<String, String>> joinRDD = rdd1.join(rdd2);
        List<Tuple2<Integer, Tuple2<String, String>>> joinResults = joinRDD.collect();
        assertEquals(Arrays.asList(
                new Tuple2<>(1, new Tuple2<>("A", "X")),
                new Tuple2<>(2, new Tuple2<>("B", "Y"))
        ), joinResults);

        // Left Outer Join
        JavaPairRDD<Integer, Tuple2<String, Optional<String>>> leftJoinRDD = rdd1.leftOuterJoin(rdd2);
        List<Tuple2<Integer, Tuple2<String, Optional<String>>>> leftJoinResults = leftJoinRDD.collect();
        assertEquals(Arrays.asList(
                new Tuple2<>(1, new Tuple2<>("A", Optional.of("X"))),
                new Tuple2<>(2, new Tuple2<>("B", Optional.of("Y"))),
                new Tuple2<>(3, new Tuple2<>("C", Optional.empty()))
        ), leftJoinResults);

        // Right Outer Join
        JavaPairRDD<Integer, Tuple2<Optional<String>, String>> rightJoinRDD = rdd1.rightOuterJoin(rdd2);
        List<Tuple2<Integer, Tuple2<Optional<String>, String>>> rightJoinResults = rightJoinRDD.collect();
        assertEquals(Arrays.asList(
                new Tuple2<>(1, new Tuple2<>(Optional.of("A"), "X")),
                new Tuple2<>(2, new Tuple2<>(Optional.of("B"), "Y")),
                new Tuple2<>(4, new Tuple2<>(Optional.empty(), "Z"))
        ), rightJoinResults);

        // CoGroup
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> cogroupRDD = rdd1.cogroup(rdd2);
        List<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<String>>>> cogroupResults = cogroupRDD.collect();
        assertTrue(cogroupResults.size() > 0);
    }

    @Test
    public void testReduceByKeyAndCombineByKey() {
        List<Tuple2<String, Integer>> data = Arrays.asList(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 2),
                new Tuple2<>("A", 3),
                new Tuple2<>("B", 4)
        );
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);

        // ReduceByKey
        JavaPairRDD<String, Integer> reducedRDD = pairRDD.reduceByKey(Integer::sum);
        List<Tuple2<String, Integer>> reducedResults = reducedRDD.collect();
        assertEquals(Arrays.asList(
                new Tuple2<>("A", 4),
                new Tuple2<>("B", 6)
        ), reducedResults);

        // CombineByKey
        JavaPairRDD<String, Tuple2<Integer, Integer>> combinedRDD = pairRDD.combineByKey(
                x -> new Tuple2<>(x, 1),
                (acc, value) -> new Tuple2<>(acc._1 + value, acc._2 + 1),
                (acc1, acc2) -> new Tuple2<>(acc1._1 + acc2._1, acc1._2 + acc2._2)
        );
        List<Tuple2<String, Tuple2<Integer, Integer>>> combinedResults = combinedRDD.collect();
        assertEquals(Arrays.asList(
                new Tuple2<>("A", new Tuple2<>(4, 2)),
                new Tuple2<>("B", new Tuple2<>(6, 2))
        ), combinedResults);
    }

    @Test
    public void testZipAndZipWithIndex() {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("apple", "banana", "cherry"));

        // Zip operation
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3));
        JavaPairRDD<String, Integer> zippedRDD = rdd.zip(rdd2);
        List<Tuple2<String, Integer>> zippedResults = zippedRDD.collect();
        assertEquals(Arrays.asList(
                new Tuple2<>("apple", 1),
                new Tuple2<>("banana", 2),
                new Tuple2<>("cherry", 3)
        ), zippedResults);

        // ZipWithIndex operation
        JavaPairRDD<String, Long> zippedWithIndexRDD = rdd.zipWithIndex();
        List<Tuple2<String, Long>> zippedWithIndexResults = zippedWithIndexRDD.collect();
        assertEquals(Arrays.asList(
                new Tuple2<>("apple", 0L),
                new Tuple2<>("banana", 1L),
                new Tuple2<>("cherry", 2L)
        ), zippedWithIndexResults);
    }
}

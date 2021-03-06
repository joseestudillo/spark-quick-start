package com.joseestudillo.spark.stream;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;
import com.joseestudillo.spark.utils.SparkUtils;

import scala.Tuple2;

/**
 * Spark stream examples.
 * <p>
 * To try with a socket server use <code>nc -lk PORT</code>
 *
 * @author Jose Estudillo
 */
public class SparkStreaming {

	private static final Logger log = Logger.getLogger(SparkStreaming.class);

	private static final int BASE_DURATION = 2;

	private static final int NC_PORT = 9999;

	private static final Duration BATCH_DURATION = Durations.seconds(BASE_DURATION);
	// window is the amount of time to check
	private static final Duration WINDOW_DURATION = Durations.seconds(BASE_DURATION * 3);
	//the slide defines how often the results are computed
	private static final Duration SLIDE_DURATION = Durations.seconds(BASE_DURATION * 1);

	public static void main(String[] args) throws Exception {
		SparkConf conf = SparkUtils.getLocalConfig(SparkStreaming.class.getSimpleName());
		log.info("access to the web interface at localhost:4040");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_DURATION);
		JavaDStream<String> dStream = jssc.socketTextStream("localhost", NC_PORT);

		//spark automatically monitor the directory, but it doesn't monitor modified files.
		//JavaDStream<String> dStream = jssc.textFileStream("/tmp/logs");

		//do the work count
		JavaPairDStream<String, Integer> pairs = dStream.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((a, b) -> a + b);

		log.info("running....");
		//wordCounts.print();
		//window(wordCounts);
		//reduceByKey(wordCounts);
		//reduceByKeyAndWindow(wordCounts);
		//countByWindow(jssc, wordCounts);
		//countByValueAndWindow(jssc, dStream);
		updateStateByKey(jssc, wordCounts);

		jssc.start();
		jssc.awaitTermination();
		log.info("done");
	}

	private static void setUpCheckpointing(JavaStreamingContext jssc) throws IOException {
		File checkpointDir = new File("/tmp/spark");
		FileUtils.deleteDirectory(checkpointDir);
		checkpointDir.mkdirs();
		jssc.checkpoint(checkpointDir.getPath());
	}

	private static void logWindowingInfo() {
		log.info(String.format("batchSize %s, windowSize: %s slideSize: %s, #Slides: %s",
				BATCH_DURATION,
				WINDOW_DURATION,
				SLIDE_DURATION,
				WINDOW_DURATION.milliseconds() / SLIDE_DURATION.milliseconds()));
	}

	/**
	 * - It will display the wordcount executed in each slide
	 * <p>
	 * - you will have windowDuration/slideDuration slides
	 * <p>
	 * - every time 'slideSize' elapses, an slide gets out of the window and a new one gets in
	 * <p>
	 *
	 * @param wordCounts
	 */
	private static void window(JavaPairDStream<String, Integer> wordCounts) {
		log.info("window:");
		logWindowingInfo();
		JavaPairDStream<String, Integer> windowedDStream = wordCounts.window(WINDOW_DURATION, SLIDE_DURATION);
		windowedDStream.print();
	}

	/**
	 * - Reduces the value in the whole Window (no slides)
	 *
	 * @param wordCounts
	 */
	private static void reduceByKey(JavaPairDStream<String, Integer> wordCounts) {
		log.info("reduceByKey:");
		Function2<Integer, Integer, Integer> reduceByKeyFunc = new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				Integer result = v1 + v2;
				log.info(String.format("reduceByKeyFunc.call(%s, %s) = ", v1, v2, result));
				return result;
			}
		};
		JavaPairDStream<String, Integer> outputDStream = wordCounts.reduceByKey(reduceByKeyFunc);
		outputDStream.print();
	}

	/**
	 * - goes through all the windows every time there is a new window in (an another out)
	 *
	 * @param wordCounts
	 */
	private static void reduceByKeyAndWindow(JavaPairDStream<String, Integer> wordCounts) {
		log.info("reduceByKeyAndWindow:");
		logWindowingInfo();
		Function2<Integer, Integer, Integer> reduceByKeyAndWindowFunc = new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				Integer result = v1 + v2;
				log.info(String.format("reduceByKeyAndWindowFunc.call(%s, %s) = ", v1, v2, result));
				return result;
			}
		};
		//noticed if the window size is bigger that the batch size and the slide is not specified, the slide will be the batch size 
		JavaPairDStream<String, Integer> outputDStream = wordCounts.reduceByKeyAndWindow(reduceByKeyAndWindowFunc, WINDOW_DURATION, SLIDE_DURATION);
		outputDStream.print();
	}

	/**
	 * @param wordCounts
	 * @throws IOException
	 */
	private static void countByWindow(JavaStreamingContext jssc, JavaPairDStream<String, Integer> wordCounts) throws IOException {
		log.info("countByWindow:");
		logWindowingInfo();
		setUpCheckpointing(jssc);
		JavaDStream<Long> outputDStream = wordCounts.countByWindow(WINDOW_DURATION, SLIDE_DURATION);
		outputDStream.print();
	}

	/**
	 * @param dStream
	 * @throws IOException
	 */
	private static void countByValueAndWindow(JavaStreamingContext jssc, JavaDStream<String> dStream) throws IOException {
		log.info("countByValueAndWindow:");
		logWindowingInfo();
		setUpCheckpointing(jssc);
		JavaPairDStream<String, Long> outputDStream = dStream.countByValueAndWindow(WINDOW_DURATION, SLIDE_DURATION);
		outputDStream.print();
	}

	/**
	 * @param jssc
	 * @param wordCounts
	 * @throws IOException
	 */
	private static void updateStateByKey(JavaStreamingContext jssc, JavaPairDStream<String, Integer> wordCounts) throws IOException {
		log.info("updateStateByKey:");
		logWindowingInfo();
		setUpCheckpointing(jssc);
		Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateStateByKeyFunction = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
			@Override
			public Optional<Integer> call(List<Integer> nTimesKeyHasAppearedInEachWindow, Optional<Integer> accumulatedValueForAKey) throws Exception {
				Optional<Integer> result;
				result = Optional.of(
						Math.max(
								nTimesKeyHasAppearedInEachWindow.stream().reduce((a, b) -> Math.max(a, b)).orElse(0),
								accumulatedValueForAKey.or(0)));
				//result = Optional.of(nTimesKeyHasAppearedInEachWindow.stream().reduce((a, b) -> a + b).orElse(0)); //value of the given window
				//result = Optional.of(accumulatedValueForAKey.or(0) + windowReduction); //adding all the values from the windows
				log.info(String.format("updateStateByKey.call(%s, %s) = %s", nTimesKeyHasAppearedInEachWindow, accumulatedValueForAKey, result));
				return result;
			}

		};
		//updateStateByKeyFunction = (l, current) -> Optional.<Integer> of(current.or(0) + l.size());

		JavaPairDStream<String, Integer> windowedDStream = wordCounts.window(WINDOW_DURATION, SLIDE_DURATION);

		JavaPairDStream<String, Integer> outputDStream = windowedDStream.updateStateByKey(updateStateByKeyFunction);
		outputDStream.print();
	}
}

package edu.campus02.iwi.lv.stream;

import static org.apache.spark.sql.functions.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.google.gson.Gson;

import spark.exercise.utils.CustomStreamQueryListener;
import spark.exercise.utils.QueryTerminator;

public class TweetsAnalyzerStructuredStreaming {

	public static final Gson GSON = new Gson();

	public static final StructType TWEET_STRUCT = new StructType()
			.add("id", DataTypes.LongType)
			.add("text", DataTypes.StringType)
			.add("lang", DataTypes.StringType)
			.add("source", DataTypes.StringType)
			.add("entities", new StructType()
					.add("hashtags",
							new ArrayType(
									new StructType().add("text", DataTypes.StringType),false
									)
							)
					);

	public static void main(String[] args) {

		SparkConf cnf = new SparkConf()
				.setMaster("local[2]")
				.setAppName(TweetsAnalyzerStructuredStreaming.class.getName())
				.set("spark.sql.streaming.checkpointLocation", "data/checkpoint/sqlstreaming")
				.set("spark.sql.shuffle.partitions", "8");

		SparkSession spark = SparkSession.builder().config(cnf).getOrCreate();

		//TODO 1: 
		//stateful counting of number of tweets per language in total
		// - create a Dataset by using the consumeFromFakeTwitterAPI(...) method
		// - use the DSL from Spark SQL to calculate the number tweets per language
		//   and sort them first based on count DESC, then based on lang ASC
		// DOCS -> https://spark.apache.org/docs/latest/sql-programming-guide.html#getting-started
		
		//TODO 2:
		// create & start StreamingQuery based on the Dataset from 1) as follows
		// - using output mode "complete"
		// - choose the "console sink"
		// - restrict number of rows to at most 100
		// - set a 5 seconds interval as processing trigger
		// - name the query "tweets count by language"
		// DOCS -> https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#starting-streaming-queries

		//TODO 3: count the hashtag occurrence within tumbling windows
		// - create a Dataset by using the consumeFromFakeTwitterAPI(...) method
		// - use the DSL from Spark SQL to select the hashtags and the processing time
		// - use withColumn method to explode the hashtags column which is an array
		// - do a windowed grouping based on the processingTime with length 10 seconds
		//   and group by hashtags
		// - use the count aggregate function
		// - filter the grouped results to only retain hashtags having a count of >= 3
		// DOCS -> https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-operations---selection-projection-aggregation

		//TODO 4:
		// create & start StreamingQuery based on the Dataset from 3) as follows
		// - using output mode "update"
		// - choose the "console sink"
		// - restrict number of rows to at most 1_000
		// - set the truncate option to false
		// - set a 10 seconds interval as processing trigger
		// - name the query "windowed hashtag counts"
		// DOCS -> https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#starting-streaming-queries

		try {
			ScheduledExecutorService ste = Executors.newSingleThreadScheduledExecutor();			

			//OPTIONAL - TODO 5
			//schedule a termination thread to run after e.g. 5 minutes 
			//for your two queries by using the ScheduledExecutorService
			//above and the pre-defined QueryTerminator class which can
			//be found in the utils package of the workspace

			spark.streams().awaitAnyTermination();
			ste.awaitTermination(10, TimeUnit.SECONDS);
			ste.shutdownNow();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private static Dataset<Row> consumeFromFakeTwitterAPI(SparkSession spark) {

		//SOCKET SOURCE ONLY FOR DEMO/TESTING PURPOSES
		//keeps in-mem buffer infinitely and has no fault tolerance built in
		Dataset<Row> raw = spark
				.readStream()
				.format("socket")
				.option("host", "localhost")
				.option("port", 9999)
				.load();

		return raw.select(from_json(col("value"), TWEET_STRUCT).as("tweet"))
				.withColumn("processingTime", current_timestamp());
	}

}

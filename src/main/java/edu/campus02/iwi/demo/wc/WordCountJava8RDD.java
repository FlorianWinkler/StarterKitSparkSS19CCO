package edu.campus02.iwi.demo.wc;

import java.util.Arrays;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import spark.exercise.env.WinConfig;

public class WordCountJava8RDD {

	public static void main(String[] args) {

		WinConfig.setupEnv();
		
		if(args.length != 2) {
			System.err.println("usage: program <input_dir> <output_dir>");
			System.exit(-1);
		}
		
		// The first thing a Spark program must do is to create a
		// SparkSession object, which tells Spark how to access
		// a cluster. To create a SparkSession you first need to build
		// a SparkConf object that contains information about your
		// application.

		// The appName parameter is a name for your application to show on the
		// cluster UI. Master is a Spark, Mesos or YARN cluster URL, or the
		// special string "local" to run in local mode. In practice, when running
		// on a cluster, you will not want to hardcode master in the program, but
		// rather launch the application with spark-submit and receive it there.
		SparkConf cnf = new SparkConf().setMaster("local[1]")
				.setAppName(WordCountJava8RDD.class.getName());

		SparkSession spark = SparkSession.builder()
			     .config(cnf)
			     .getOrCreate();

		JavaRDD<String> lines = spark.read().text(args[0]+"/*.txt")
									.as(Encoders.STRING()).toJavaRDD();
		
        JavaPairRDD<String, Integer> counts = 
	        		//typically you would also normalize
					//and apply stop word filtering as well...
	        		lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
        						.mapToPair(word -> new Tuple2<>(word,1))
        							.reduceByKey((c1,c2) -> c1+c2);

		//saving our resulting JavaPairRDD with word and total count to local filesystem
        String uuid = UUID.randomUUID().toString();
		String destination = args[1]+"/run/"+uuid+"/txt/";
		System.out.println("saving result to "+destination);
		counts.saveAsTextFile(destination);
		
		// It's good practice to explicitly stop the context before
		// your program finishes :)
		spark.close();
		
	}

}

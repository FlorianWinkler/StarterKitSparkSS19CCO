package edu.campus02.iwi.lv.warmup;

import java.util.Arrays;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import spark.exercise.env.WinConfig;

public class EnhancedWordCount {

	public static void main(String[] args) {

		WinConfig.setupEnv();
		
		if(args.length != 2) {
			System.err.println("usage: program <input_dir> <output_dir>");
			System.exit(-1);
		}
		
		SparkConf cnf = new SparkConf().setMaster("local[1]")
				.setAppName(EnhancedWordCount.class.getName());

		JavaSparkContext spark = new JavaSparkContext(cnf);

		JavaRDD<String> lines = spark.textFile(args[0]+"/*.txt");
		
        JavaPairRDD<String, Integer> wordCounts = 
	        		//TODO 2: modify behavior of tokenization of lines
	        		lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
        			 	//TODO 3: add filter for non-empty words
	        			.mapToPair(word -> new Tuple2<>(word,1))
        				.reduceByKey((c1,c2) -> c1+c2);

        //TODO 4 & 5: swap Key <-> Value within Tuple2 to sort by Key    
     
        //TODO 6: write top N to console
        
		//saving our resulting JavaPairRDD with word and total count to local filesystem
        String uuid = UUID.randomUUID().toString();
		String destination = args[1]+"/run/"+uuid+"/txt/";
		System.out.println("saving result to "+destination);
		//TODO 7: JavaPairRDD from 5) should get written to the txt file
		//XYZ.saveAsTextFile(destination);
		
		// It's good practice to explicitly stop the context before
		// your program finishes :)
		spark.close();
		
	}

}

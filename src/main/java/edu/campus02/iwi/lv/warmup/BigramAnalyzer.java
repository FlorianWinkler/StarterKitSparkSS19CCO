package edu.campus02.iwi.lv.warmup;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import shapeless.Tuple;
import spark.exercise.env.WinConfig;

public class BigramAnalyzer {

	public static void main(String[] args) {
		
		WinConfig.setupEnv();
		
		if(args.length != 1) {
			System.err.println("usage: program <input_dir>");
			System.exit(-1);
		}
		
		SparkConf cnf = new SparkConf().setMaster("local[1]")
				.setAppName(BigramAnalyzer.class.getName());

		JavaSparkContext spark = new JavaSparkContext(cnf);
			     
		JavaRDD<String> lines = spark.textFile(args[0]+"/*.txt");
									
        JavaPairRDD<String, Integer> bigramCounts = 
	        		lines.flatMap(line -> {
	        			//TODO 1: toLowerCase, character removal and split by whitespace
						String[] wordlist = line.toLowerCase().
								replaceAll("[^a-z ]","").
								split(" ");
	        			
	        			List<String> bigrams = new ArrayList<>();
	        			//TODO 2: build list with all bigrams of String[] from above
						for(int i=0; i<wordlist.length-1;i++){
							if(!wordlist[i].isEmpty() && !wordlist[i+1].isEmpty())
								bigrams.add(wordlist[i]+" "+wordlist[i+1]);
						}
	        			
	        			return bigrams.iterator();
	        		})
	        		.mapToPair(word -> new Tuple2<>(word,1))
	        			.reduceByKey((c1,c2) -> c1+c2);
	    
        //TODO 3: swap tuples, sort by counts desc and take first
        //to write the most frequent bigram to console
		List<Tuple2<Integer,String>> topBigram =
				bigramCounts.mapToPair(Tuple2::swap)
				.sortByKey(false)
				.take(5);

		System.out.println(topBigram);
        
		spark.close();

	}

}

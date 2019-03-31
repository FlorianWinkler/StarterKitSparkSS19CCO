package edu.campus02.iwi.lv.warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import spark.exercise.env.WinConfig;

public class CoTags {

	public static void main(String[] args) {
		
		WinConfig.setupEnv();
		
		if(args.length != 2) {
			System.err.println("usage: program <input_dir> <output_dir>");
			System.exit(-1);
		}
		
		SparkConf cnf = new SparkConf().setMaster("local[2]")
				.setAppName(CoTags.class.getName());

		JavaSparkContext spark = new JavaSparkContext(cnf);
		
		//TODO 1: create a JavaRDD<String> based on the file sample.txt
		
									
		//TODO 2: create all possible tag pairs and group to count the pairs
		
		 
		//TODO 3: check a handful of entries to see if you are doing it right :)
		
        
        //TODO 4: restructure the data so that you can group by the first tag of the tag pairs tuple
        
        
        //TODO 5: write the resulting pair RDD from 4) into a file
       
        spark.close();
		
	}

}

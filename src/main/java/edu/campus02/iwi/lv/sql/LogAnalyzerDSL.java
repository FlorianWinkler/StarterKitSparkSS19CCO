package edu.campus02.iwi.lv.sql;

import static org.apache.spark.sql.functions.*;

import java.util.List;
import java.util.Objects;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import spark.exercise.env.WinConfig;


public class LogAnalyzerDSL {

	public static void main(String[] args) {

		WinConfig.setupEnv();
		
		if (args.length != 1) {
			System.err.println("usage: program <path_to_apache_log_file>");
			System.exit(-1);
		}

		//TODO 1: create spark config & session instead of context
		SparkConf conf = new SparkConf().setMaster("local")
				.setAppName(LogAnalyzerDSL.class.getName());
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		//TODO 2: read in the access log file with string encoder
		//and map the elements with the bean encoder to ApacheAccessLog objects
		//we also filter them in order to keep only non-null entries
		Dataset<ApacheAccessLog> accessLogs = spark.read().text(args[0])
				.as(Encoders.STRING())
				.map(ApacheAccessLog::parseFromLogLine,Encoders.bean(ApacheAccessLog.class))
				.filter(Objects::nonNull).cache();
		
		//TODO 3: inspect the schema and a few sample records
		accessLogs.printSchema();
		
		//TODO 4: simple grouping by HTTP method
		//accessLogs.groupBy("ipAddress").count().show();
		
		//TODO 5: calculate and show the top 25 requested resources (=endpoints)
		//accessLogs.groupBy("endpoint").count().
		//		orderBy("count").show(25,false);
		
		//TODO 6: collect all hosts (=ipAddress) that have accessed
		//the server more than 250 times into  list and it to the console
		accessLogs.groupBy("ipAddress").count().filter(col("count").gt(250))
				.orderBy(col("count").desc()).show();
		
		//TODO 7: apply the aggregate functions avg, min, max, sum
		//based on contentSize column of requested resources
		//and show the results
		accessLogs.agg(min("contentSize"),
				max("contentSize"),
				sum("contentSize"),
				avg("contentSize"))
			.show();
		
		
	}
}
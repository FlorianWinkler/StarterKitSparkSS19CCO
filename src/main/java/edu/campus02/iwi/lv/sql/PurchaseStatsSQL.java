package edu.campus02.iwi.lv.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import spark.exercise.env.WinConfig;

public class PurchaseStatsSQL {

	public static void main(String[] args) {

		WinConfig.setupEnv();
		
		if (args.length != 1) {
			System.err.println("usage: program <path_to_purchase_json>");
			System.exit(-1);
		}

		//TODO 1: create spark config & session instead of context
		SparkConf conf = new SparkConf().setMaster("local").setAppName(PurchaseStatsSQL.class.getName());
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();



		//TODO 2: read in json data file as a dataframe (=Dataset<Row>)
		//and directly cache it...
		Dataset<Row> buyings= spark.read().json(args[0]).cache();

		//TODO 3: register a temporary view named "buyings"
		//which can later be used in our SQL statements...
		buyings.createOrReplaceTempView("buyings");

		//TODO 4: inspect the schema of the dataframe and show some records
		//NOW LETS WRITE SOME good old plain SQL :)

		Dataset<Row> ds = spark.sql("select * from buyings");
		ds.show(5);
		
		//TODO 5: write SQL to get the number of records
		//grouped for each payment type
        ds = spark.sql("select count(*) from buyings");
        //ds.show();
		
		//TODO 6: write SQL to find the categories AVG orderTotal for any purchases
		//within the productCategory "Music" or "Books" which were paid
		//by "Visa"
        ds = spark.sql("select productCategory, avg(orderTotal) " +
                "from buyings " +
                "where productCategory in ('Music', 'Books') and paymentType = 'Visa' " +
                "group by(productCategory)");
        //ds.show();
		
		//TODO 7: write SQL to find highest and lowest orderTotal for any purchases
		//done in "San Diego" which were paid by "Cash"
        ds = spark.sql("select buyingLocation, min(orderTotal), max(orderTotal) " +
                "from buyings " +
                "where buyingLocation = 'San Diego' and paymentType = 'Cash' " +
                "group by (buyingLocation)");
        ds.show();
	}

}

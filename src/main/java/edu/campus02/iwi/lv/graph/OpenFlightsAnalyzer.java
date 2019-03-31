package edu.campus02.iwi.lv.graph;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import spark.exercise.env.WinConfig;
import spire.math.interval.Open;

public class OpenFlightsAnalyzer {

  public static void main(String[] args) {

    WinConfig.setupEnv();

    //TODO 1: spark config and session
    SparkConf config = new SparkConf().setMaster("local").setAppName(OpenFlightsAnalyzer.class.getName());
    SparkSession spark = SparkSession.builder().config(config).getOrCreate();

    //TODO 2:
    //a) airports DF
    Dataset<Row> airports = spark.read()
            .option("header",true)
            .option("delimiter",",")
            .option("nullValue", "\\N")
            .option("inferSchema", true)
            .csv("./data/input/lv/openflights/airports.dat");
    airports.printSchema();
    airports.show(10);


    //b) airlines DF
    Dataset<Row> airlines = spark.read()
            .option("header",true)
            .option("delimiter",",")
            .option("nullValue", "\\N")
            .option("inferSchema", true)
            .csv("./data/input/lv/openflights/airlines.dat");
    airports.printSchema();
    airlines.show(10);

    //c) routes DF
    Dataset<Row> routes = spark.read()
            .option("header",true)
            .option("delimiter",",")
            .option("nullValue", "\\N")
            .option("inferSchema", true)
            .csv("./data/input/lv/openflights/routes.dat");
    airports.printSchema();
    routes.show(10);

    //TODO 3: create GF based on DFs
    GraphFrame gf = new GraphFrame(
            airports.selectExpr("AirportID as id","City"),
            routes.selectExpr("SourceAirportID as src", "DestinationAirportID as dst", "AirlineID","Stops")
    );

    //TODO 4:
    //TOP 10 US airports having highest number of incoming flights
    gf.inDegrees()
            .join(airports,col("id").equalTo(col("AirportID")))
            .filter(airports.col("Country").equalTo("United States"))
            .orderBy(col("inDegree").desc())
            .show(10);

    //TODO 5: all flights within Austria only
    //V1:
    //a) filter for airports in Austria

    //b) JOIN to the routes DF twice the airport DF (1x for src, 1x for dst)
    //then the airline DF

    //V2:
    //motif-finding and 1 join enrichment

    //TODO 6 BONUSTASK
    //all flights from VIE -> SFO but with either Lufthansa/Austrian/Swiss and 2 hops only

  }

}
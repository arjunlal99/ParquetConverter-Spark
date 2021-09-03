package com.converter.parser

import com.converter.util.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class TsvParser extends Parser {
    override def parse(): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val tsvDF = spark.read
            .format("csv")
            .option("delimiter", "\t")
            .option("inferSchema", "true")
            .load(Configuration.inputDir + "/*")

        //sequence of required columns to be parsed from the log
        val columnSeq = Configuration.lookup.keys.toSeq.map(m => col(m))
        val reducedDF = tsvDF.select(columnSeq: _*)

        //sequence or required columns renamed as per configuration file
        val renamedColumnSeq = reducedDF.columns.map(c => col(c).as(Configuration.lookup.getOrElse(c,c)))
        val finalDF = reducedDF.select(renamedColumnSeq: _*)

        finalDF.write
            .format("parquet")
            .mode("overwrite")
            .save(Configuration.outputDir + "/" + Configuration.outputFilename)


    }
}

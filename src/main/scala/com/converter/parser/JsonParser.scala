package com.converter.parser

import com.converter.util.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class JsonParser extends Parser {
    override def parse(): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val jsonDF = spark.read
            .format("json")
            .option("inferSchema", "true")
            .load(Configuration.inputDir + "/*")

        val columnSeq = jsonDF.columns.map(c => col(c).as(Configuration.lookup.getOrElse(c,c)))
        val finalDF = jsonDF.select(columnSeq: _*)

        finalDF.write
            .format("parquet")
            .mode("overwrite")
            .save(Configuration.outputDir + "/" + Configuration.outputFilename)

    }
}

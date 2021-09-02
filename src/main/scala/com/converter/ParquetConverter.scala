package com.converter

import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import com.converter.util.Configuration

object ParquetConverter {

    private val LOGGER = LogManager.getLogger(ParquetConverter.getClass)

    def main(args: Array[String]): Unit ={
        val spark = SparkSession.builder.appName("Parquet Converter").getOrCreate()

        Configuration.parseConfiguration(args(0))


    }
}

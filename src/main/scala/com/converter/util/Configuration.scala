package com.converter.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{column, explode}
import org.apache.log4j.LogManager

import scala.collection.mutable.ArrayBuffer

object Configuration{

    private val LOGGER = LogManager.getLogger(this.getClass)

    var inputFileFormat: String = _
    var inputDir: String = _
    var outputDir: String = _
    var outputFilename: String = _
    var fields: ArrayBuffer[Field] = ArrayBuffer[Field]()

    def parseConfiguration(path: String): Unit ={
        val spark = SparkSession.builder().getOrCreate()
        val configurationDF = spark.read.format("json").option("multiline", "true").load(path)

        val dataDF = configurationDF.select("inputFileFormat","inputDir","outputDir","outputFilename")
        val dataArray = dataDF.collect()(0)
        this.inputFileFormat = dataArray(0).toString
        this.inputDir = dataArray(1).toString
        this.outputDir = dataArray(2).toString
        this.outputFilename = dataArray(3).toString

        val fieldsDF = configurationDF.select(explode(column("fields")) as "fields").select("fields.*")
        val fieldsArray = fieldsDF.collect()

        for (field <- fieldsArray){
            val fieldInstance = Field(field(0).toString.toInt, field(1).toString, field(2).toString)
            this.fields.append(fieldInstance)
        }

    }
}

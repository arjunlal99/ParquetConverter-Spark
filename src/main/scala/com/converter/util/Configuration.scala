package com.converter.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{column, explode}
import org.apache.log4j.LogManager

import scala.collection.mutable.ArrayBuffer
import java.nio.file.{Files,Paths}
import com.converter.exceptions.ConfigurationFileNotFoundException

object Configuration{

    private val LOGGER = LogManager.getLogger(this.getClass)

    var inputFileFormat: String = _
    var inputDir: String = _
    var outputDir: String = _
    var outputFilename: String = _
    var fields: ArrayBuffer[Field] = ArrayBuffer[Field]()
    var lookup: Map[String, String] = Map() //lookup Map for column index to column name

    def parseConfiguration(path: String): Unit ={
        val spark = SparkSession.builder().getOrCreate()
        try{
            if (! Files.exists(Paths.get(path))) throw new ConfigurationFileNotFoundException()
        } catch {
            case c: ConfigurationFileNotFoundException =>
                LOGGER.error("ConfigurationFileNotFoundException : Program cannot find configuration file at the given location - provide correct path to a valid configuration file")
                System.exit(-1)
        }

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
            //add name to lookup map
            this.lookup += ("_c" + fieldInstance.Index -> fieldInstance.Name)
        }

    }


}

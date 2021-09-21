package com.converter.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import com.converter.exceptions.ConfigurationFileNotFoundException
import com.converter.util.Configuration.{setFields, setProperties}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class ConfigurationTestSuite extends AnyFunSuite with BeforeAndAfterAll {

    var spark: SparkSession = _

    override def beforeAll(): Unit ={
        spark = SparkSession
            .builder
            .appName("Parquet Converter Test")
            .config("spark.master", "local")
            .getOrCreate()
    }

    override def afterAll(): Unit = {
        spark.stop()
    }

    test("An invalid file should throw ConfigurationFileNotFoundException"){
        assertThrows[ConfigurationFileNotFoundException]{
            Configuration.checkConfigurationFileExists("/invalid/path/conf.json")
        }
    }

    test("setProperties function should set inputDir, outputDir, inputFileFormat and outputFilename properties of Configuration Object"){
        val path = getClass.getResource("/conf.json").getPath
        val configurationDF = spark.read.format("json").option("multiline", "true").load(path)
        setProperties(spark, configurationDF)
        assert(Configuration.inputDir === "/input")
        assert(Configuration.outputDir === "/output")
        assert(Configuration.inputFileFormat === "tsv")
        assert(Configuration.outputFilename === "dhcp-log")
    }



}

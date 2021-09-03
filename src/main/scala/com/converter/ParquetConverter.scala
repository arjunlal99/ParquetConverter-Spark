package com.converter

import com.converter.exceptions.ExpectedArgumentNotFoundException
import com.converter.parser.Parser
import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import com.converter.util.Configuration

object ParquetConverter {

    private val LOGGER = LogManager.getLogger(ParquetConverter.getClass)

    def main(args: Array[String]): Unit ={
        val spark = SparkSession.builder.appName("Parquet Converter").getOrCreate()
        try{
            if (args.length == 0) throw new ExpectedArgumentNotFoundException()
        } catch {
            case c: ExpectedArgumentNotFoundException =>
                LOGGER.error("ExpectedArgumentNotFoundException : Path to configuration file expected as first argument - correct usage is spark-submit (`options`) <.jar file> </path/to/conf_file>")
                System.exit(-1)
        }

        val path: String = args(0)
        Configuration.parseConfiguration(path)

        val reader = Parser(Configuration.inputFileFormat)
        reader.parse()
    }
}

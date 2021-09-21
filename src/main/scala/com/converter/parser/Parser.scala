package com.converter.parser

import org.apache.log4j.LogManager

abstract class Parser {
    def parse()
}

object Parser {

    private val LOGGER = LogManager.getLogger(this.getClass)

    def apply(inputFileFormat: String): Parser = {
        inputFileFormat.toLowerCase() match {
            case "csv" => new CsvParser()
            case "tsv" => new TsvParser()
            case "json" => new JsonParser()
        }
    }

}

# ParquetConverter

Spark job written in Scala to read CSV, TSV or JSON logs files and write required fields to parquet file. The program behavior is fully driven by a JSON configuration file.

## Configuration File
A valid JSON configuration file is to be passed as argument when submitting the Spark job.
The configuration file must contain 4 fields:
* inputFileFormat : Format of the input log files (Expects tsv, csv or json )
* inputDir : Absolute path to the directory containing the input file logs
* outputDir : Absolute path to the directory which output parquet files are written
* outputFilename : Filename for output parquet file
* fields : Array of Field objects which need to be written to output parquet file
	* Field object : Describes a field in the input log. Each Field object consists of 3 fields.
		* name: Name of the field
		* type: Expected type of the data in the field
		* index: Index of the field starting from 0

Example configuration file:
```
{  
  "inputFileFormat" : "tsv",  
  "inputDir": "/input",  
  "outputDir": "/output",  
  "outputFilename": "dhcp-log",  
  "fields": [{  
    "name" : "ts",  
  "type" : "double",  
  "index": "0"  
  },  
  {  
      "name" : "uid",  
  "type" : "string",  
  "index" : "1"  
  },  
  {  
      "name" : "id_resp_h",  
  "type" : "string",  
  "index" : "4"  
  },  
  
  {  
      "name" : "assigned_ip",  
  "type" : "string",  
  "index" : "7"  
  }]  
}
```

## Compiling Program

The program is compiled using sbt build tool. Run the following command from inside project directory:
```
sbt package
```
The jar file will be outputted to the target folder of the project
## Submitting Spark Job
The program can be run on Spark using spark-submit from the command line in the following format: 
```
$SPARK_HOME/bin/spark-submit \
  --class com.converter.ParquetConverter \
  --master <master-url> \
  <application-jar> \
  /path/to/configuration_file
```
Example:
```
$SPARK_HOME/bin/spark-submit \
  --class com.converter.ParquetConverter \
  --master local \
  parquetconverter.jar \
  /mnt/conf.json
```


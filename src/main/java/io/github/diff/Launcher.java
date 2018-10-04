package io.github.diff;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import scala.Tuple3;
import scala.collection.JavaConverters;

public class Launcher {

	public static void main(String[] args) {
		

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		
		Options options = new Options();
		options.addOption( "op", "old-path", true, "path of the old csv file (could be hdfs:/// or file:/// or relative path" );
		options.addOption( "np", "new-path", true, "path of the new csv file (could be hdfs:/// or file:/// or relative path" );
		options.addOption( "cp", "created-path", true, "output csv file path of the new rows (could be hdfs:/// or file:/// or relative path" );
		options.addOption( "up", "updated-path", true, "output csv file path of the updated rows(could be hdfs:/// or file:/// or relative path" );
		options.addOption( "dp", "deleted-path", true, "output csv file path of the deleted rows (could be hdfs:/// or file:/// or relative path" );
		options.addOption(Option.builder("pk").longOpt("primary-keys").desc("primary keys of the input csv files").hasArgs().build());
		
		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);

			if (line.hasOption("old-path") && line.hasOption("new-path") && line.hasOption("created-path")
					&& line.hasOption("updated-path") && line.hasOption("deleted-path")
					&& line.hasOption("primary-keys")) {

				SparkSession spark = SparkSession.builder().appName("CSV diff").config("spark.ui.enabled", false)
						.master("local[*]").getOrCreate();

				Dataset<Row> dfold = spark.read().format("csv").option("header", "true").option("mode", "FAILFAST").load(line.getOptionValue("old-path"));
				Dataset<Row> dfnew = spark.read().format("csv").option("header", "true").option("mode", "FAILFAST").load(line.getOptionValue("new-path"));
				List<String> pks = Arrays.asList(line.getOptionValues("primary-keys"));
				Tuple3<Dataset<Row>, Dataset<Row>, Dataset<Row>> resp = DiffDataframe.diff(dfold, dfnew,
						JavaConverters.asScalaIteratorConverter(pks.iterator()).asScala().toSet());

				resp._1().coalesce(1).write().mode(SaveMode.Overwrite).format("csv").option("header", "true").save(line.getOptionValue("created-path"));
				resp._2().coalesce(1).write().mode(SaveMode.Overwrite).format("csv").option("header", "true").save(line.getOptionValue("deleted-path"));
				resp._3().coalesce(1).write().mode(SaveMode.Overwrite).format("csv").option("header", "true").save(line.getOptionValue("updated-path"));

			} else {
				formatter.printHelp("csv-diff", options);
			}
		} catch (ParseException exp) {
			System.out.println("Unexpected exception:" + exp.getMessage());
			formatter.printHelp("csv-diff", options);
		}
		
	}

}

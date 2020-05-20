package it.unipi.hadoop;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import it.unipi.hadoop.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans
{
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "k-means");
		job.setJarByClass(KMeans.class);
		job.setMapperClass(KMeansMapper.class);
		/*job.setCombinerClass(KMeansCombiner.class);
		job.setReducerClass(KMeansReducer.class);*/
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans
{
	public static void main(String[] args) throws Exception
	{
		final Configuration conf = new Configuration();

		final Job job = new Job(conf, "k-means");
		job.setJarByClass(KMeans.class);
		job.setMapperClass(KMeansMapper.class);
		//job.setCombinerClass(KMeansCombiner.class);
		job.setReducerClass(KMeansReducer.class);
		
		//setta i path di input e output
		//la cartella di input va prima create sul dfs con: hadoop fs mkdir -p /Resource/Input
		//e vanno inseriti i file di input con: hadoop fs -put ./Resources/Input/points.txt ./Resources/Input/clusters.txt /Resources/Input
		//oppure lo si può fare direttamente dall'interfaccia online
		FileInputFormat.addInputPath(job, new Path("Resources/Input"));
		FileOutputFormat.setOutputPath(job, new Path("Resources/Output"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

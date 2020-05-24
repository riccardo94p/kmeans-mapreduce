package it.unipi.hadoop;

import it.unipi.hadoop.writables.Centroid;
import it.unipi.hadoop.writables.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.util.Arrays;

public class KMeans
{
	public static void main(String[] args) throws Exception
	{
		final Configuration conf = new Configuration();

		final Job job = new Job(conf, "k-means");
		job.setJarByClass(KMeans.class);

		job.setMapperClass(KMeansMapper.class);
		job.setMapOutputKeyClass(Centroid.class);
		job.setMapOutputValueClass(Point.class);

		job.setCombinerClass(KMeansCombiner.class);
		job.setReducerClass(KMeansReducer.class);
		
		//setta i path di input e output
		//la cartella di input va prima create sul dfs con: hadoop fs mkdir -p /Resource/Input
		//e vanno inseriti i file di input con: hadoop fs -put ./Resources/Input/points.txt ./Resources/Input/clusters.txt /Resources/Input
		//oppure lo si può fare direttamente dall'interfaccia online
		FileInputFormat.addInputPath(job, new Path("Resources/Input/points.txt"));
		System.out.println(Arrays.toString(FileInputFormat.getInputPaths(job)));
		FileSystem.get(conf).delete(new Path("Resources/Output"), true);
		FileOutputFormat.setOutputPath(job, new Path("Resources/Output"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

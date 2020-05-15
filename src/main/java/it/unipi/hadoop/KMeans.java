package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import it.unipi.hadoop.*;

public class KMeans
{
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "k-means");
		job.setJarByClass(KMeans.class);
		//job.setMapperClass(KMeansMapper.class);
		job.setCombinerClass(KMeansCombiner.class);
		job.setReducerClass(KMeansReducer.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

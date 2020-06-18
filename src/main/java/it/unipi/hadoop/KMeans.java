package it.unipi.hadoop;

import it.unipi.hadoop.writables.Centroid;
import it.unipi.hadoop.writables.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.*;
import java.io.IOException;
import java.util.Arrays;

public class KMeans
{
	public final static int MAX_ITER = 3;
	public final static float THRESHOLD = 0.1f;

	private static Job createJob(Configuration conf, String name) throws IOException {
		Job job = new Job(conf, name);
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
		FileSystem.get(conf).delete(new Path("Resources/Output"), true);
		FileOutputFormat.setOutputPath(job, new Path("Resources/Output"));

		return job;
	}

	private static String readCentroids(Configuration conf, String path) throws IOException {
		String clusters = null;
		FileSystem fs = FileSystem.get(conf);
		InputStreamReader ir = new InputStreamReader(fs.open(new Path(path)));
		BufferedReader br = new BufferedReader(ir);

		int i = 0;
		String line = br.readLine();
		while (line != null){
			if(clusters == null) clusters = line;
			else clusters = clusters +"\n"+line;
			i++;
			line = br.readLine();
		}
		//removes unwanted characters from output file
		clusters = clusters.replace("[", "");
		clusters = clusters.replace("]", "");
		clusters = clusters.replace(",", "");

		return clusters;
	}

	public static void main(String[] args) throws Exception
	{
		final Configuration conf = new Configuration();

		int iter = 0;
		String centroids = readCentroids(conf, "Resources/Input/clusters.txt");
		String oldCentroids = "";

		while(iter < MAX_ITER) {
			System.out.println("Iteration "+iter);

			conf.set("centroids", centroids);

			//job deve essere creato DOPO aver finito di impostare tutti i parametri della configurazione
			final Job job = createJob(conf, "k-means");
			job.waitForCompletion(true);

			oldCentroids = centroids;

			//read new centroids
			centroids = readCentroids(conf, "Resources/Output/part-r-00000");

			iter++;
		}
	}
}

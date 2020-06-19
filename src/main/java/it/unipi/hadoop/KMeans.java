package it.unipi.hadoop;

import it.unipi.hadoop.Iterators.CentroidList;
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
import java.util.ArrayList;
import java.util.List;

public class KMeans
{
	public final static int MAX_ITER = 10;
	public final static double THRESHOLD = 0.2; //java requires F or f after float value otherwise it treats is as double
	public static int NUM_CENTROIDS = 0;

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
		//set the number of centroids K
		if(NUM_CENTROIDS == 0) NUM_CENTROIDS = i;

		//removes unwanted characters from output file
		clusters = clusters.replace("[", "");
		clusters = clusters.replace("]", "");
		clusters = clusters.replace(",", "");

		return clusters;
	}

	//computes the difference between the new centroids and the ones of the previous iteration
	//if the variation is under a certain threshold, we can consider the algorithm as converged
	private static double computeVariation(String prec, String curr) throws Exception {
		if(prec.equals("") || curr.equals("")) return Double.MAX_VALUE;

		CentroidList old = new CentroidList();
		CentroidList current = new CentroidList();

		BufferedReader reader = new BufferedReader(new StringReader(prec));
		BufferedReader reader2 = new BufferedReader(new StringReader(curr));
		String line = reader.readLine(), line2 = reader2.readLine();

		while(line != null && line2 != null) {
			old.add(line);
			current.add(line2);

			line = reader.readLine();
			line2 = reader2.readLine();
		}

		double variation = 0.0;

		for(int i =0; i<old.getCentroids().size(); i++)
			variation += old.getCentroids().get(i).getPoint().getDistance(current.getCentroids().get(i).getPoint());

		return variation;
	}

	public static void main(String[] args) throws Exception
	{
		final Configuration conf = new Configuration();

		int iter = 0;
		String centroids = readCentroids(conf, "Resources/Input/clusters.txt");
		String oldCentroids = "";
		double var = 0.0;

		while(iter < MAX_ITER && ((var = computeVariation(oldCentroids, centroids)) > THRESHOLD)) {
			iter++;
			System.out.println("\n############## Iteration "+iter+" #######################");
			System.out.println("###### VARIATION: "+ var+"\n");

			conf.set("centroids", centroids);

			//job deve essere creato DOPO aver finito di impostare tutti i parametri della configurazione
			final Job job = createJob(conf, "k-means");
			job.waitForCompletion(true);

			oldCentroids = centroids;

			//read new centroids
			centroids = readCentroids(conf, "Resources/Output/part-r-00000");
		}

		System.out.println("\n######################### RESULT ##########################");
		System.out.println("K-Means MapReduce converged after "+iter+" iterations.\n");
	}
}
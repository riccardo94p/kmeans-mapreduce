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
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class KMeans
{
	public final static int MAX_ITER = 10;
	public final static double THRESHOLD = 0.03;
	public static int NUM_CENTROIDS;
	public static String INPUT_PATH = "Resources/Input";
	public static String OUTPUT_PATH = "Resources/Output";
	public static String inputCentroidsFile;
	public static String inputPointsFile;

	private static Job createJob(Configuration conf, String name) throws IOException {
		Job job = new Job(conf, name);
		job.setJarByClass(KMeans.class);

		job.setMapperClass(KMeansMapper.class);
		job.setMapOutputKeyClass(Centroid.class);
		job.setMapOutputValueClass(Point.class);

		job.setCombinerClass(KMeansCombiner.class);
		job.setReducerClass(KMeansReducer.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH+"/"+inputPointsFile));
		FileSystem.get(conf).delete(new Path(OUTPUT_PATH), true);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

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

		return (variation/NUM_CENTROIDS); //returns the mean distance between old and new centroids
	}

	public static void main(String[] args) throws Exception
	{
		NUM_CENTROIDS = (!args[0].equals(""))? Integer.parseInt(args[0]) : 3;
		inputCentroidsFile = (!args[0].equals("") && !args[2].equals(""))? "centroids_"+args[0]+"x"+args[2]+".txt": "centroids_3x3.txt";
		inputPointsFile = (!args[1].equals("") && !args[2].equals(""))? "points_"+args[1]+"x"+args[2]+".txt" : "points_100000x3.txt";

		System.out.println("[DBG]: ");
		System.out.println("Centroids File: "+inputCentroidsFile);
		System.out.println("Points File: "+inputPointsFile);

		long start = System.currentTimeMillis();
		final Configuration conf = new Configuration();

		int iter = 0;
		//read centroids from input files
		String centroids = readCentroids(conf, INPUT_PATH+"/"+inputCentroidsFile);//"Resources/Input/centroidsx7.txt");
		String oldCentroids = "";
		double var = 0.0;

		while(iter < MAX_ITER && ((var = computeVariation(oldCentroids, centroids)) > THRESHOLD)) {
			iter++;
			System.out.println("\n############## Iteration "+iter+" #######################");
			System.out.println("###### VARIATION: "+ var+"\n");

			conf.set("centroids", centroids);

			final Job job = createJob(conf, "k-means");
			job.waitForCompletion(true);

			oldCentroids = centroids;

			//read new centroids
			centroids = readCentroids(conf, OUTPUT_PATH+"/part-r-00000");
		}
		long end = System.currentTimeMillis();
		float elapsedTime = (end - start)/1000f; //convert to seconds

		String text = NUM_CENTROIDS+" "+args[1]+" "+args[2]+" "+elapsedTime+" "+iter+"\n"; //num_centroids, num_points, dimension (x,y,z..), elapsed_time, num_iterations
		try {
			Files.write(Paths.get("hadoop_results.txt"), text.getBytes(), StandardOpenOption.APPEND);
		}catch (IOException e) { e.printStackTrace(); }
	}
}
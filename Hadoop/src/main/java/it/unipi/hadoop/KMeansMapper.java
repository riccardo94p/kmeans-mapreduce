package it.unipi.hadoop;

import it.unipi.hadoop.Iterators.CentroidList;
import it.unipi.hadoop.writables.Centroid;
import it.unipi.hadoop.writables.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.Arrays;

public class KMeansMapper extends Mapper<LongWritable, Text, Centroid, Point> {

	private CentroidList centList;
	private Point point;

	//Function to initialize Point and CentroidList before map function
	@Override
	protected void setup(Context context) throws IOException {
		point = new Point();
		centList = new CentroidList();

		BufferedReader reader = new BufferedReader(new StringReader(context.getConfiguration().get("centroids")));
		String line = reader.readLine();
		while(line != null) {
			centList.add(line);
			line = reader.readLine();
		}
	}

	//Map function
	 public void map(LongWritable key, Text value, Context context) {
		 try {
			 //retrieve the input point
			 point.parse(value.toString());
		 	 //emit the nearest centroid to the input point
			 context.write(centList.closest(point), point);
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
	 }
}

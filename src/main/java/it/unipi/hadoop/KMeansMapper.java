package it.unipi.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.hadoop.writables.Centroid;
import it.unipi.hadoop.writables.Point;

public class KMeansMapper extends Mapper<Text, Text, Centroid, Point> {
	
	/*
	 * key(Text) -> list of coordinates
	 * value(Text) -> null
	 */
	
	private Centroid centroid = new Centroid();
	private Point point = new Point();
	
	 public void map(Text key, Text value, Context context) {

	  }
	    
	
}

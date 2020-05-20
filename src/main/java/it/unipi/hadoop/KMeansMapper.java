package it.unipi.hadoop;

import it.unipi.hadoop.Iterators.CentroidList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.hadoop.writables.Centroid;
import it.unipi.hadoop.writables.Point;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KMeansMapper extends Mapper<Text, Text, Centroid, Point> {
	
	/*
	 * key(Text) -> list of coordinates
	 * value(Text) -> null
	 */

	private CentroidList centList;
	private Point point;

	@Override
	protected void setup(Context context) throws IOException {
		point = new Point();
		centList = new CentroidList();

/* 		Recupero centroidi dal cache distribuita dovrebbe funzionare cosi
		List<URI> uris = Arrays.asList(context.getCacheFiles());
		for (URI uri: uris){
			FileSystem fs = FileSystem.get(context.getConfiguration());
			InputStreamReader ir = new InputStreamReader(fs.open(new Path(uri)));
			BufferedReader br = new BufferedReader(ir);

			String line = br.readLine();
			while (line != null){
				centList.add(line);
				line = br.readLine();
			}

		}*/
	}

	 public void map(Text key, Text value, Context context) {
		//recupera il punto in input
		point.parse(value.toString());
		 try {
		 	 //emette il centroide più vicino e il punto
			 context.write(centList.closest(point), point);
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
	 }
}

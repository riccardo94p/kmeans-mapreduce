package it.unipi.hadoop;

import it.unipi.hadoop.Iterators.CentroidList;
import it.unipi.hadoop.writables.Centroid;
import it.unipi.hadoop.writables.Point;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class KMeansMapper extends Mapper<LongWritable, Text, Centroid, Point> {
	
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

 		//Recupero centroidi dal cache distribuita dovrebbe funzionare cosi
		//List<URI> uris = Arrays.asList(context.getCacheFiles());

		//for (URI uri: uris){
			FileSystem fs = FileSystem.get(context.getConfiguration());/*
			InputStreamReader ir = new InputStreamReader(fs.open(new Path(uri)));*/
			InputStreamReader ir = new InputStreamReader(fs.open(new Path("Resources/Input/clusters.txt")));
			BufferedReader br = new BufferedReader(ir);

			String line = br.readLine();
			while (line != null){
				centList.add(line);
				line = br.readLine();
			}

		//}
	}

	 public void map(LongWritable key, Text value, Context context) {
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

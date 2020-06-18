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

			//InputStreamReader ir = new InputStreamReader(fs.open(new Path(uri)));
			//per renderlo iterativo bisogna passare i punti in un altro modo (esempio conf.setStrings(..)) perchè allo stato attuale legge sempre i punti iniziali
			/*System.out.println("################### TEST  ###########################");
			System.out.println(context.getConfiguration().get("centroids"));

			FileSystem fs = FileSystem.get(context.getConfiguration());
			InputStreamReader ir = new InputStreamReader(fs.open(new Path("Resources/Input/clusters.txt")));
			BufferedReader br = new BufferedReader(ir);

			int i = 0;
			String line = br.readLine();
			while (line != null){
				centList.add(line);
				System.out.println(centList.getCentroids().get(i).getId().toString());
				System.out.println(Arrays.toString(centList.getCentroids().get(i).getPoint().getCoordinates()));
				i++;
				line = br.readLine();
			}*/
		BufferedReader reader = new BufferedReader(new StringReader(context.getConfiguration().get("centroids")));
		String line = reader.readLine();
		while(line != null) {
			centList.add(line);
			line = reader.readLine();
		}

		//}
	}

	 public void map(LongWritable key, Text value, Context context) {
		 try {
			 //recupera il punto in input
			 point.parse(value.toString());
		 	 //emette il centroide più vicino e il punto
			 //System.out.println(centList.closest(point).getId().toString());
			 context.write(centList.closest(point), point);
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
	 }
}

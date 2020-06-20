package it.unipi.hadoop;

import it.unipi.hadoop.writables.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class KMeansCombiner extends Reducer<Centroid, Point, Centroid, Point>
{
	@Override
	protected void reduce(Centroid key, Iterable<Point> val, Context context) throws IOException, InterruptedException
	{
		Iterator<Point> it = val.iterator();
		Point pTot = new Point(it.next());
		
		while(it.hasNext()) {
			Point p = it.next();
			pTot.add(p);
		}
		//pass the data to the reducer
		context.write(key, pTot);
	}
}
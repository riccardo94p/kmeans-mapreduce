package it.unipi.hadoop;

import it.unipi.hadoop.writables.Centroid;
import it.unipi.hadoop.writables.Point;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Iterator;

public class KMeansReducer extends Reducer<Centroid, Point, Text, NullWritable>
{
    Text newKey = new Text();

    //Reduce function
    @Override
    protected void reduce(Centroid key, Iterable<Point> values, Context context) {
        Iterator<Point> it = values.iterator();

        Point pTot = new Point(it.next());
        while(it.hasNext()){
            Point p = it.next();
            pTot.add(p);
        }

        //setting new centroid 
        newKey.set(new Centroid(key.getId(), computeMean(pTot)).toString());

        try {
        	//emit the index of the cluster, the new centroid (newKey), and a null placeholder
             context.write(newKey, NullWritable.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    //Computes the new coordinates of the centroid as the mean of the coordinates 
    //of the points belonging to the centroid's cluster
    public Point computeMean(Point p) {
        Point out = new Point();
        double[] vec = new double[p.getCoordinates().length];
        for(int i = 0; i < p.getCoordinates().length; i++) {
            vec[i] = p.getCoordinates()[i] / p.getCount();
        }

        out.setCoordinates(vec);
        return out;
    }
}
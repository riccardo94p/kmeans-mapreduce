package it.unipi.hadoop;

import it.unipi.hadoop.writables.Centroid;
import it.unipi.hadoop.writables.Point;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class KMeansReducer extends Reducer<Centroid, Point, Text, NullWritable>
{
    Text newKey = new Text();

    //calcola il nuovo centroide dalla somma dei punti e il numero di punti del cluster
    public Point computeMean(Point p) {
        Point out = new Point();
        double[] vec = new double[p.getCoordinates().length];
        for(int i = 0; i < p.getCoordinates().length; i++) {
            vec[i] = p.getCoordinates()[i] / p.getCount();
        }

        out.setCoordinates(vec);
        return out;
    }

    @Override
    protected void reduce(Centroid key, Iterable<Point> values, Context context) {
        Iterator<Point> it = values.iterator();

        Point pTot = new Point(it.next());
        while(it.hasNext()){
            Point p = it.next();
            pTot.add(p);
        }
        /*System.out.println(Arrays.toString(pTot.getCoordinates()));
        System.out.println(pTot.getCount());*/

        //setta il nuovo centroide con id e media dei punti nel cluster
        newKey.set(new Centroid(key.getId(), computeMean(pTot)).toString());

        try {
             //emette l'indice del cluster e il nuovo centroide (newKey) e un placeholder vuoto (NullWritable.Get())
             context.write(newKey, NullWritable.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

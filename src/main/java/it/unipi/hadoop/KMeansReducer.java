package it.unipi.hadoop;

import it.unipi.hadoop.writables.Centroid;
import it.unipi.hadoop.writables.Point;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<Centroid, Point, Centroid, Point>
{
}

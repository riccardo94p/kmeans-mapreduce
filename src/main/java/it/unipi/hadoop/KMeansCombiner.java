package it.unipi.hadoop;

import it.unipi.hadoop.writables.*;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansCombiner extends Reducer<Centroid, Point, Centroid, Point>
{
}

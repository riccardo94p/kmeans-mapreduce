package it.unipi.hadoop.Iterators;

import it.unipi.hadoop.writables.Centroid;
import it.unipi.hadoop.writables.Point;

import java.util.ArrayList;
import java.util.List;

public class CentroidList {
    List<Centroid> centroids;

    public CentroidList(){
        centroids = new ArrayList<>();
    }

    public List<Centroid> getCentroids(){
        return this.centroids;
    }

    //Function to add a new centroid in the list extracted from a string
    public void add(String line){
        centroids.add(new Centroid(line));
    }

    //Function that finds the nearest centroid from the point passed as argument
    public Centroid closest(Point p) throws Exception {
    	if(centroids.size() == 0)
    		return new Centroid();
    	
        double d = Double.POSITIVE_INFINITY;
        Centroid c = null;
        
        for(int i = 0; i < centroids.size(); i++){
            if(d > p.getDistance(centroids.get(i).getPoint())) {
                d = p.getDistance(centroids.get(i).getPoint());
                c = centroids.get(i);
            }
        }
        
        return c;
    }
}

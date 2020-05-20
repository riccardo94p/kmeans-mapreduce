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

    public void add(String line){
        centroids.add(new Centroid());
    }

    //funzione che trova il centroide più vicino
    public Centroid closest(Point p) throws Exception {
        double d = Double.POSITIVE_INFINITY;
        Centroid c = new Centroid();
        
        for(int i = 0; i < centroids.size(); i++){
            if(d > p.getDistance(centroids.get(i).getPoint())) {
                d = p.getDistance(centroids.get(i).getPoint());
                c = centroids.get(i);
            }
        }
        
        return c;
    }
}

package it.unipi.hadoop.writables;

public class Centroid {
	private Point p;
	
	Centroid() {
		p = new Point();
	}
	
	Centroid(Point point) {
		p = new Point(point);
	}
}

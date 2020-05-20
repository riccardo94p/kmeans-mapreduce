package it.unipi.hadoop.writables;

public class Centroid {
	private Point p;
	
	public Centroid() {
		p = new Point();
	}
	
	public Centroid(Point point) {
		p = new Point(point);
	}

	public Point getPoint(){
		return this.p;
	}
}



/*public class Centroid extends Point {
 
	private int id;
	
	public Centroid() {
		super();
		this.id = 0;
	}
	
	public Centroid(Point p, int id) {
		super(p);
		this.id = id;
	}
	
	public int getId(){
		return this.id;
	}
	
	public void setId(int id){
		this.id = id;
	}
}*/
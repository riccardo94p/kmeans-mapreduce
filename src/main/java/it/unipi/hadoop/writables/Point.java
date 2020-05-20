package it.unipi.hadoop.writables;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point implements Writable
{
	private ArrayPrimitiveWritable coordinates = null; //array of the values of the coordinates of this point (or sum of points)
	private IntWritable count; //counts how many points are summed up
	
	//private List<Double> coordinates = null;
	//private int count;
	
	public Point() {
		coordinates = new ArrayPrimitiveWritable();
		count = new IntWritable(1);
	}
	
	Point(Point p) {
		this();
		coordinates.set(p.getCoordinates());
		count.set(p.getCount());
	}
	
/*	public Point(String coordinates){
		
		String[] splitCoordinates = coordinates.split(",");

		for(int i=0; i< splitCoordinates.length; i++) {
			coordinates.add(Double.parseDouble(splitCoordinates[i].trim()));
		}
		
	}
*/
	
	
	public double[] getCoordinates() { return (double[]) coordinates.get();	}
	public int getCount() { return (int) count.get(); }
	public void setCoordinates(double[] vector) { this.coordinates.set(vector); }
	public void setCount(int c) { this.count.set(c); }
	
	public void add(Point p) { //add point p to this
		double[] thisPoint = this.getCoordinates();
		double[] point = p.getCoordinates();
		
		//add up all the coordinates
		for(int i=0; i<thisPoint.length; i++)
			thisPoint[i] += point[i];
		//update the count at the end
		count.set(this.count.get() + (int) p.getCount());
	}
	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		coordinates.write(dataOutput);
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		coordinates.readFields(dataInput);
	}
	
	public double getDistance(Point otherPoint) { //computes Euclidean distance between this point and otherPoint
		double distance = 0.0;
		double[] coordinatesPoint = this.getCoordinates();
		double[] coordinatesOtherPoint = otherPoint.getCoordinates();

		if(coordinatesPoint.length != coordinatesOtherPoint.length){
			System.out.println("Points in different dimension space");
			return Double.POSITIVE_INFINITY;
		}

		for(int i = 0; i < coordinatesPoint.length; i++){
			distance += Math.pow(coordinatesPoint[i] -  coordinatesOtherPoint[i], 2);
		}
		distance = Math.sqrt(distance);

		return distance;
	}
}

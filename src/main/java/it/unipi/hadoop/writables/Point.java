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
	private IntWritable count = null; //counts how many points are summed up
	
	//private List<Double> coordinates = null;
	//private int count;
	
	public Point() {
		coordinates = new ArrayPrimitiveWritable();
		count = new IntWritable(0);
	}
	
	public Point(Point p) {
		this();
		setCoordinates(p.getCoordinates());
		setCount((int)p.getCount());
	}
	
/*	public Point(String coordinates){
		
		String[] splitCoordinates = coordinates.split(",");

		for(int i=0; i< splitCoordinates.length; i++) {
			coordinates.add(Double.parseDouble(splitCoordinates[i].trim()));
		}
		
	}
*/
	
	
	public double[] getCoordinates() { return (double[]) coordinates.get();	}
	public double getCount() { return (double) count.get(); }
	public void setCoordinates(double[] vector) { this.coordinates.set(vector); }
	public void setCount(int c) { this.count.set(c); }
	
	public void add(Point p) { //add point p to this
		double[] thisPoint = this.getCoordinates();
		double[] point = p.getCoordinates();
		
		//add up all the coordinates
		for(int i=0; i < thisPoint.length; i++)
			thisPoint[i] += point[i];
		//update the count at the end
		count.set(this.count.get() + (int) p.getCount());
	}

	//funzione emettere il Point da Mapper/Combiner
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		coordinates.write(dataOutput);
		count.write(dataOutput);
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		coordinates.readFields(dataInput);
		count.readFields(dataInput);
	}
	
	public double getDistance(Point otherPoint) throws Exception { //computes Euclidean distance between this point and otherPoint
		double distance = 0.0;
		double[] coordinatesPoint = this.getCoordinates();
		double[] coordinatesOtherPoint = otherPoint.getCoordinates();

		if(coordinatesPoint.length != coordinatesOtherPoint.length) throw new Exception("Points in different dimension spaces");

		for(int i = 0; i < coordinatesPoint.length; i++){
			distance += Math.pow(coordinatesPoint[i] -  coordinatesOtherPoint[i], 2);
		}

		return Math.sqrt(distance);
	}

	//scrive un punto da una stringa
	public void parse(String values){
		String[] vector = values.split(" ");
		double[] tmp = new double[vector.length];

		for(int i = 0; i < vector.length; i++) {
			tmp[i] = Double.valueOf(vector[i]);
		}

		this.setCoordinates(tmp);
		this.setCount(1);
	}
}

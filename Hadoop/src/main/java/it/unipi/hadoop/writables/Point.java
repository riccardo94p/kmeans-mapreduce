package it.unipi.hadoop.writables;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point implements Writable
{
	//array of the values of the coordinates of this point (or sum of points)
	private double[] coordinates;
	
	//counts how many points are summed up
	private int count;
	
	public Point() {
		coordinates = null;
		count = 0;
	}
	
	public Point(Point p) {
		setCoordinates(p.getCoordinates());
		setCount((int)p.getCount());
	}
	
	//Getter and Setter
	public double[] getCoordinates() { return coordinates;	}
	public int getCount() { return count; }
	public void setCoordinates(double[] vector) { coordinates = vector; }
	public void setCount(int c) { count = c; }
	
	
	//Function to sum a Point with this point
	public void add(Point p) { 
		double[] thisPoint = this.getCoordinates();
		double[] point = p.getCoordinates();
		
		//add up all the coordinates
		for(int i=0; i < thisPoint.length; i++)
			thisPoint[i] += point[i];
		//update the count at the end
		count = count + p.getCount();
	}

	//Serialization for emit point
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		ArrayPrimitiveWritable c = new ArrayPrimitiveWritable();
		c.set(coordinates);
		c.write(dataOutput);
		dataOutput.writeInt(count);
	}
	
	//Deserialization
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		ArrayPrimitiveWritable c = new ArrayPrimitiveWritable();
		c.readFields(dataInput);
		coordinates = (double[])c.get();
		count = dataInput.readInt();
	}
	
	//Computes Euclidean distance between this point and otherPoint
	public double getDistance(Point otherPoint) throws Exception { 
		double distance = 0.0;
		double[] coordinatesOtherPoint = otherPoint.getCoordinates();

		if(coordinates.length != coordinatesOtherPoint.length) throw new Exception("Points in different dimension spaces");

		for(int i = 0; i < coordinates.length; i++){
			distance += Math.pow(coordinates[i] -  coordinatesOtherPoint[i], 2);
		}
		return Math.sqrt(distance);
	}

	//Extracts a Point from a string
	public void parse(String values){
		String[] vector = values.split(" ");
		coordinates = new double[vector.length];

		for(int i = 0; i < vector.length; i++) {
			coordinates[i] = Double.valueOf(vector[i]);
		}
		
		count = 1;
	}
}
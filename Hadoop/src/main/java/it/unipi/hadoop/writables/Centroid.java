package it.unipi.hadoop.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Centroid implements WritableComparable<Centroid> {
	private Text id;
	private Point p;
	
	public Centroid() {
		id = new Text();
		p = new Point();
	}
	
	public Centroid(Text id, Point point) {
		this.id = id;
		p = new Point(point);
	}

	//Constructor to create a centroid from a string of a text file
	public Centroid(String value){
		this();

		int index = value.indexOf("\t");
		if (index == -1) {
			index = value.indexOf(" ");
		}
		id.set(value.substring(0, index).replace(".", ""));
		p.parse((value.substring(index + 1)));
	}
	
	
	//Getter
	public Text getId() {return this.id; }

	public Point getPoint(){
		return this.p;
	}

	//Function to print the centroid (id + coordinates)
	@Override
	public String toString() {
		return this.getId() + ". " + Arrays.toString(this.getPoint().getCoordinates());
	}

	//Function to compare two centroids (due to WritableComparable implementation)
	@Override
	public int compareTo(Centroid centroid) {
		return this.id.compareTo(centroid.getId());
	}

	//Serialization for emit centroid
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		id.write(dataOutput);
		p.write(dataOutput);
	}

	//Deserialization
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		id.readFields(dataInput);
		p.readFields(dataInput);
	}
}

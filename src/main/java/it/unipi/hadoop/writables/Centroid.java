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

	//costruttore per creare un centroide da una stringa di un file di testo
	public Centroid(String value){
		this();

		int index = value.indexOf("\t");
		if (index == -1) {
			index = value.indexOf(" ");
		}
		id.set(value.substring(0, index).replace(".", ""));
		p.parse((value.substring(index + 1)));
	}

	public Text getId() {return this.id; }

	public Point getPoint(){
		return this.p;
	}

	//funzione per stampare il cluster con id e coordinate del centroide
	@Override
	public String toString() {
		return this.getId() + ". " + Arrays.toString(this.getPoint().getCoordinates());
	}

	@Override
	public int compareTo(Centroid centroid) {
		return this.id.compareTo(centroid.getId());
	}

	//serve per emettere il centroide da Mapper/Combiner
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		id.write(dataOutput);
		p.write(dataOutput);
	}

	//serve per leggere i centroide in ingresso a Mapper/Reducer/Combiner
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		id.readFields(dataInput);
		p.readFields(dataInput);
	}
}
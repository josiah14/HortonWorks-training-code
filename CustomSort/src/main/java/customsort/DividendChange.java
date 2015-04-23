package customsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DividendChange implements Writable {
	private String symbol;
	private String date;
	private double change;

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public double getChange() {
		return change;
	}

	public void setChange(double change) {
		this.change = change;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(symbol);
		out.writeUTF(date);
		out.writeDouble(change);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		symbol = in.readUTF();
		date = in.readUTF();
		change = in.readDouble();
	}
	
	@Override
	public String toString() {
		return symbol + "\t" + date + "\t" + change;
	}
}

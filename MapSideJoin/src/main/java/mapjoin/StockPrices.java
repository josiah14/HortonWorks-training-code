package mapjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StockPrices implements Writable {
	
	private double dividend;
	private double closingPrice;

	public StockPrices() {}
	
	public StockPrices(double dividend, double closingPrice) {
		this.dividend = dividend;
		this.closingPrice = closingPrice;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		dividend = in.readDouble();
		closingPrice = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(dividend);
		out.writeDouble(closingPrice);
	}

	public double getDividend() {
		return dividend;
	}

	public void setDividend(double dividend) {
		this.dividend = dividend;
	}

	public double getClosingPrice() {
		return closingPrice;
	}

	public void setClosingPrice(double closingPrice) {
		this.closingPrice = closingPrice;
	}

	@Override
	public String toString() {
		return dividend + "," + closingPrice;
	}
}

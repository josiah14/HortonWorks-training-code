package average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StockPrices implements Writable {
	private double open, high, low, close, adjustedClose;
	private int volume;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		open = in.readDouble();
		high = in.readDouble();
		low = in.readDouble();
		close = in.readDouble();
		adjustedClose = in.readDouble();
		volume = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(open);
		out.writeDouble(high);
		out.writeDouble(low);
		out.writeDouble(close);
		out.writeDouble(adjustedClose);
		out.writeInt(volume);
	}

	public double getOpen() {
		return open;
	}

	public void setOpen(double open) {
		this.open = open;
	}

	public double getHigh() {
		return high;
	}

	public void setHigh(double high) {
		this.high = high;
	}

	public double getLow() {
		return low;
	}

	public void setLow(double low) {
		this.low = low;
	}

	public double getClose() {
		return close;
	}

	public void setClose(double close) {
		this.close = close;
	}

	public double getAdjustedClose() {
		return adjustedClose;
	}

	public void setAdjustedClose(double adjustedClose) {
		this.adjustedClose = adjustedClose;
	}

	public int getVolume() {
		return volume;
	}

	public void setVolume(int volume) {
		this.volume = volume;
	}
}

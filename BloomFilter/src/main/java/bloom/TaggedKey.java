package bloom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public abstract class TaggedKey<K extends WritableComparable<K>> implements
    WritableComparable<TaggedKey<K>> {
  private int tag = 0;

  public TaggedKey() {
  }

  public TaggedKey(int tag) {
    this.tag = tag;
  }

  public int getTag() {
    return tag;
  }

  public abstract K getKey();
   
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(tag);
    getKey().write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tag = in.readInt();
    getKey().readFields(in);
  }
  
  @Override
  public int compareTo(TaggedKey<K> o) {
    int result = getKey().compareTo(o.getKey());
    return result != 0 ? result : (tag - o.tag);
  }

  @Override
  public String toString() {
    return String.format("%s: %s\n", tag, getKey());
  }

}

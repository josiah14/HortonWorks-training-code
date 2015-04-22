package bloom;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public abstract class TaggedKeyGroupingComparator extends WritableComparator {
  public TaggedKeyGroupingComparator(Class<? extends WritableComparable<?>> keyClass) {
    super(keyClass, true);
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    TaggedKey lhs = (TaggedKey) a;
    TaggedKey rhs = (TaggedKey) b;
    return lhs.getKey().compareTo(rhs.getKey());
  }

}
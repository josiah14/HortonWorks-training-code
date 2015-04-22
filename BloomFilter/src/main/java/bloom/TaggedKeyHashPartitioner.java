package bloom;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class TaggedKeyHashPartitioner<K extends WritableComparable<K>, V>
    extends Partitioner<TaggedKey<K>, V> {

  HashPartitioner<K, V> partitioner = new HashPartitioner<K, V>();

  @Override
  public int getPartition(TaggedKey<K> key, V value, int numPartitions) {
    return partitioner.getPartition(key.getKey(), value, numPartitions);
  }
}
package bloom;

public class StockTaggedKey extends TaggedKey<Stock> {

  protected Stock key;
  
  public StockTaggedKey() {
    key = new Stock();
  }
  
  public StockTaggedKey(int tag, Stock key) {
    super(tag);
    this.key = key;
  }
 
  @Override
  public Stock getKey() {
    return key;
  }
  
}

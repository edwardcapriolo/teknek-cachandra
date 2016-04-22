

public interface DirectSource<Key,Value> {
  long getGetCount();
  Value get(Key key);
}

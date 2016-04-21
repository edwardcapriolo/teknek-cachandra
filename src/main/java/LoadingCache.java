

public interface LoadingCache<Key,Value> {
  Value get(Key key);
}

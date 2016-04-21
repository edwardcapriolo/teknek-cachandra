
public interface CacheSource {
  long getSetCount();
  long getGetCount();
  String get(String key);
  void set(String key, String value);
}

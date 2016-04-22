
public interface CacheSource {
  String get(String key);
  void set(String key, String value);
}

import java.util.HashMap;
import java.util.Map;

public class InMemoryStorageSource implements CacheSource{

  private Map<String,String> data = new HashMap<>();
  
  @Override
  public String get(String key) {
    return data.get(key);
    
  }

  @Override
  public void set(String key, String value) {
    data.put(key, value);
    
  }
}

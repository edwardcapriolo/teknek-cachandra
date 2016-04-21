import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryStorageSource implements CacheSource{

  private Map<String,String> data = new HashMap<>();
  private AtomicLong getCount = new AtomicLong(0);
  private AtomicLong setCount = new AtomicLong(0);
  
  @Override
  public String get(String key) {
    return data.get(key);
    
  }

  @Override
  public void set(String key, String value) {
    data.put(key, value);
    
  }

  @Override
  public long getSetCount() {
    return setCount.get();
  }

  @Override
  public long getGetCount() {
    return getCount.get();
  }

}

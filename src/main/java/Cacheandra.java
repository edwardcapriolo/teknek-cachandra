import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Cacheandra<Key,Value> {

  private final DirectSource<Key,Value> directSource;
  private final ObjectMapper om;
  private final CacheSource cacheSource;
  private final TypeReference<Value> valueTypeReference;
  private final AtomicLong directSourceGetCount = new AtomicLong(0);
  private final AtomicLong cacheSourceGetCount = new AtomicLong(0);
  private final AtomicLong cacheSourceSetCount = new AtomicLong(0);

  public Cacheandra(DirectSource<Key,Value> loadingCache, 
          CacheSource storageSource, TypeReference<Value> valueRef){
    this(loadingCache, new ObjectMapper(), storageSource, valueRef );
  }
  
  public Cacheandra(DirectSource<Key,Value> directSource, 
          ObjectMapper om, CacheSource cacheSource, TypeReference<Value> valueRef){
    this.directSource = directSource;
    this.om = om;
    this.cacheSource = cacheSource;
    valueTypeReference = valueRef;
  }
  
  public Value get(Key key){
    String search;
    try {
      search = om.writeValueAsString(key);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String result = cacheSource.get(search);
    cacheSourceGetCount.getAndIncrement();
    if (result != null){
      try {
        return (Value) om.readValue(result, valueTypeReference);
      } catch ( IOException e) {
        throw new RuntimeException(e);
      }
    } else  {
      Value v = directSource.get(key);
      directSourceGetCount.getAndIncrement();
      try {
        cacheSource.set(search, om.writeValueAsString(v));
        cacheSourceSetCount.getAndIncrement();
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      return v;
    }
  }

  public long getDirectSourceGetCount() {
    return directSourceGetCount.get();
  }

  public long getCacheSourceGetCount() {
    return cacheSourceGetCount.get();
  }

  public long getCacheSourceSetCount() {
    return cacheSourceSetCount.get();
  }
  
}

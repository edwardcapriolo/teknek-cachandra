import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Cacheandra<Key,Value> {

  private final LoadingCache<Key,Value> loadingCache;
  private final ObjectMapper om;
  private final CacheSource storageSource;
  private final com.fasterxml.jackson.core.type.TypeReference<Value> valueTypeReference;
  private final com.fasterxml.jackson.core.type.TypeReference<Key> keyTypeReference;

  public Cacheandra(LoadingCache<Key,Value> loadingCache, 
          CacheSource storageSource){
    this(loadingCache, new ObjectMapper(), storageSource);
  }
  
  public Cacheandra(LoadingCache<Key,Value> loadingCache, 
          ObjectMapper om, CacheSource storageSource ){
    this.loadingCache = loadingCache;
    this.om = om;
    this.storageSource = storageSource;
    valueTypeReference = new TypeReference<Value>(){};
    keyTypeReference = new TypeReference<Key>(){};
  }
  
  public Value get(Key key){
    String search;
    try {
      search = om.writeValueAsString(key);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String result = storageSource.get(search);
    if (result != null){
      try {
        return (Value) om.readValue(result, valueTypeReference);
      } catch ( IOException e) {
        throw new RuntimeException(e);
      }
    } else  {
      Value v = loadingCache.get(key);
      try {
        storageSource.set(search, om.writeValueAsString(v));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      return v;
    }
  }
}

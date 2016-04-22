import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Cacheandra<Key,Value> {

  private final DirectSource<Key,Value> directSource;
  private final ObjectMapper om;
  private final CacheSource cacheSource;
  private final TypeReference<Value> valueTypeReference;

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
    if (result != null){
      try {
        return (Value) om.readValue(result, valueTypeReference);
      } catch ( IOException e) {
        throw new RuntimeException(e);
      }
    } else  {
      Value v = directSource.get(key);
      try {
        cacheSource.set(search, om.writeValueAsString(v));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      return v;
    }
  }
}

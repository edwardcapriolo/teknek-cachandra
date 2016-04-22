import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EndToEndTest extends AbstractFarsandra {
  
  @Test
  public void testShutdownWithLatch() throws InterruptedException {
    assertTrue("Cassandra is not running", fs.getManager().isRunning());
    CqlCacheSource source = new CqlCacheSource("test", "cache", new String [] {"127.0.0.1"}, 40);
    DirectSource<String,String> lc = new DirectSource<String,String>(){
      private AtomicLong getCounter = new AtomicLong(0);
      @Override
      public String get(String key) {
        getCounter.incrementAndGet();
        //querying real datastore
        return key + key; 
      }
      @Override
      public long getGetCount() {
        return getCounter.get();
      }};
    Cacheandra<String,String> c = new Cacheandra<>(lc, new ObjectMapper(), 
            source, new TypeReference<String>(){ });
    Assert.assertEquals("hoho", c.get("ho"));
    Assert.assertEquals(1, source.getSetCount());
    Assert.assertEquals(1, lc.getGetCount());
    Assert.assertEquals("hoho", c.get("ho"));
    Assert.assertEquals(1, source.getSetCount());
    Assert.assertEquals(2, source.getGetCount());
    Assert.assertEquals(1, lc.getGetCount());Assert.assertEquals(1, lc.getGetCount());
  }
  
  @Test
  public void testShutdownWithCustomTypes() throws InterruptedException {
    assertTrue("Cassandra is not running", fs.getManager().isRunning());
    CqlCacheSource source = new CqlCacheSource("test", "cache", new String [] {"127.0.0.1"}, 40);
    DirectSource<String,String> lc = new DirectSource<String,String>(){
      private AtomicLong getCounter = new AtomicLong(0);
      @Override
      public String get(String key) {
        getCounter.incrementAndGet();
        //querying real datastore
        return key + key; 
      }
      @Override
      public long getGetCount() {
        return getCounter.get();
      }};
    Cacheandra<String,String> c = new Cacheandra<>(lc, new ObjectMapper(), 
            source, new TypeReference<String>(){ });
    Assert.assertEquals("hoho", c.get("ho"));
    Assert.assertEquals(1, source.getSetCount());
    Assert.assertEquals(1, lc.getGetCount());
    Assert.assertEquals("hoho", c.get("ho"));
    Assert.assertEquals(1, source.getSetCount());
    Assert.assertEquals(2, source.getGetCount());
    Assert.assertEquals(1, lc.getGetCount());Assert.assertEquals(1, lc.getGetCount());
  }
  
  
  public static class Wombat {
    private String x;
    private int y;
    public Wombat(){
      
    }
    public Wombat(String x1, int y1){
      x = x1;
      y = y1;
    }

    public String getX() {
      return x;
    }
    public void setX(String x) {
      this.x = x;
    }
    public int getY() {
      return y;
    }
    public void setY(int y) {
      this.y = y;
    }
  }
  
  public static class Foobar {
    private int z;
    public Foobar(){
      
    }
    public Foobar(int z){
      this.z = z;
    }
    public int getZ() {
      return z;
    }
    public void setZ(int z) {
      this.z = z;
    }
  }
  
  @Test
  public void complexTypes(){
    CqlCacheSource source = new CqlCacheSource("test", "cache", new String [] {"127.0.0.1"}, 40);
    DirectSource<Foobar,Wombat> lc = new DirectSource<Foobar,Wombat>(){
      private AtomicLong getCounter = new AtomicLong(0);
      @Override
      public long getGetCount() {
        return getCounter.get();
      }
      @Override
      public Wombat get(Foobar key) {
        getCounter.incrementAndGet();
        return new Wombat("bzzz...calculating", key.z);
      }};
     Cacheandra<Foobar,Wombat> c = new Cacheandra<>(lc, new ObjectMapper(), source, new TypeReference<Wombat>(){ });
     Assert.assertEquals(5, c.get(new Foobar(5)).getY()) ;
     Assert.assertEquals(1, source.getSetCount());
     Assert.assertEquals(1, lc.getGetCount());
     Assert.assertEquals(5, c.get(new Foobar(5)).getY()) ;
     Assert.assertEquals(1, source.getSetCount());
     Assert.assertEquals(2, source.getGetCount());
     Assert.assertEquals(1, lc.getGetCount());
     Assert.assertEquals(1, lc.getGetCount());
  }
  
}

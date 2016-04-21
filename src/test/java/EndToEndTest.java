import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.teknek.farsandra.Farsandra;
import io.teknek.farsandra.LineHandler;
import io.teknek.farsandra.ProcessHandler;

public class EndToEndTest {
  Farsandra fs;
  
  @Before
  public void setup() throws InterruptedException{
    fs = new Farsandra();
    fs.withVersion("2.2.4");
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName("target/3_1");
    fs.withCreateConfigurationFiles(true);
    fs.withHost("127.0.0.1");
    fs.withSeeds(Arrays.asList("127.0.0.1"));
    fs.withJmxPort(9999);   
    final CountDownLatch started = new CountDownLatch(1);
    fs.getManager().addOutLineHandler( new LineHandler(){
        @Override
        public void handleLine(String line) {
          System.out.println("out "+line);
          if (line.contains("Listening for thrift clients...")){
            started.countDown();
          }
        }
      } 
    );
    fs.getManager().addProcessHandler(new ProcessHandler() { 
      @Override
      public void handleTermination(int exitValue) {
        System.out.println("Cassandra terminated with exit value: " + exitValue);
        started.countDown();
      }
    });
    fs.start();
    started.await(10, TimeUnit.SECONDS);
  }
  
  @After
  public void close(){
    if (fs != null){
      try {
        fs.getManager().destroyAndWaitForShutdown(6);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  @Test
  public void testShutdownWithLatch() throws InterruptedException {
    assertTrue("Cassandra is not running", fs.getManager().isRunning());
    CqlCacheSource source = new CqlCacheSource("test", "cache", 40);
    LoadingCache<String,String> lc = new LoadingCache<String,String>(){

      @Override
      public String get(String key) {
        return key + key;
        
      }};
    Cacheandra<String,String> c = new Cacheandra<>(lc, new ObjectMapper(), source);
    Assert.assertEquals("hoho", c.get("ho"));
    Assert.assertEquals(1, source.getSetCount());
    Assert.assertEquals("hoho", c.get("ho"));
    Assert.assertEquals(1, source.getSetCount());
    Assert.assertEquals(2, source.getGetCount());
  }
}

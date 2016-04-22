import io.teknek.farsandra.Farsandra;
import io.teknek.farsandra.LineHandler;
import io.teknek.farsandra.ProcessHandler;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;


public class AbstractFarsandra {

  protected Farsandra fs;
  
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
}

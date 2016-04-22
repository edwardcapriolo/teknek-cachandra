
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CqlCacheSource implements CacheSource {

  private final Cluster cluster;
  private final Session session;
  private final PreparedStatement preparedGet;
  private final PreparedStatement preparedSet;
  private final int ttlSeconds;
  private AtomicLong getCount = new AtomicLong(0);
  private AtomicLong setCount = new AtomicLong(0);
  
  public CqlCacheSource (String keyspace, String cacheName, String [] hosts, int ttlSeconds){
    cluster = Cluster.builder().addContactPoints(hosts).build();
    session = cluster.connect();
    session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + 
" WITH REPLICATION ={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
    session.execute("USE " + keyspace);
    session.execute("CREATE TABLE IF NOT EXISTS " + cacheName + " (key text, value text, PRIMARY KEY (key)) ");
    preparedGet = session.prepare
            ("SELECT value FROM " + cacheName + " WHERE key = ?");
    preparedSet = session.prepare
            ("INSERT INTO " + cacheName + " (key,value) VALUES (?,?) USING TTL ?");
    this.ttlSeconds = ttlSeconds;
  }
  
  @Override
  public String get(String key) {
    getCount.incrementAndGet();
    ResultSet rs = session.execute(preparedGet.bind(key));
    Row r = rs.one();
    if (r != null){
      return r.getString("value");
    } else {
      return null;
    }
  }

  @Override
  public void set(String key, String value) {
    setCount.incrementAndGet();
    session.execute(preparedSet.bind(key, value, ttlSeconds));
  }

}

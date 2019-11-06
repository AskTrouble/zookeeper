package mu;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;


/**
 * addauth digest username:password
 * setAcl /test auth:username:cdrwa
 * getAcl /test
 *
 */
public class Zookeeper_Acl_Create implements Watcher {

    public static String CONNECTION_IP = "127.0.0.1:2181";
  
    private static CountDownLatch latch = new CountDownLatch(1);
      
    private static CountDownLatch countDownLatch = new CountDownLatch(1);  
      
    private static ZooKeeper zk = null;
  
    public void syncInit() {  
        try {  
            zk = new ZooKeeper(CONNECTION_IP, 3000,
                    new Zookeeper_Acl_Create());  
            latch.await();
            zk.addAuthInfo("digest", "username:password".getBytes());
            Stat s = zk.exists("/act", false);
            if(s != null){
                zk.delete("/act", s.getVersion());
            }

            zk.create("/act", "init".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);

            ZooKeeper zk3 =  new ZooKeeper(CONNECTION_IP, 3000,
                    null);  
            zk3.addAuthInfo("digest", "username:password".getBytes());  
            String value2 = new String(zk3.getData("/act", false, null));  
            System.out.println("zk3有权限进行数据的获取" + value2);


            String tt = new String(zk.getData("/act", false, null));
            System.out.println("zk有权限进行数据的获取" + tt);


            ZooKeeper zk2 =  new ZooKeeper(CONNECTION_IP, 3000,
                    null);  
            zk2.addAuthInfo("digest", "super:123".getBytes());  
            zk2.getData("/act", false, null);


        } catch (InterruptedException e) {  
            e.printStackTrace();  
        } catch (IOException e) {
            e.printStackTrace();  
        } catch (KeeperException e) {
            e.printStackTrace();
            System.out.println("异常:" + e.getMessage());  
            System.out.println("zk2没有权限进行数据的获取");  
            countDownLatch.countDown();  
        }  
    }  
  
    @Override  
    public void process(WatchedEvent event) {  
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (event.getType() == Event.EventType.None && null == event.getPath()) {
                latch.countDown();  
            }   
        }  
    }  
      
    public static void main(String[] args) throws InterruptedException {  
        Zookeeper_Acl_Create acl_Create = new Zookeeper_Acl_Create();  
        acl_Create.syncInit();  
        countDownLatch.await();  
    }  
  
}
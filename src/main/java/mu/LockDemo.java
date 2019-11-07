package mu;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LockDemo {


    /**
     * zk实现的分布式锁
     */
    public static class ZooKeeperLock implements Watcher {

        public static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLock.class);

        private static final String LOCK_PATH = "/lock";

        private ZooKeeper client = new ZooKeeper("127.0.0.1:2181", 3000, this, false);
        private Object lock = new Object();

        public ZooKeeperLock() throws IOException {
            while (!client.getState().isConnected()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.error("", e);
                }
            }
        }

        public void lock() throws Exception {
            boolean success = false;
            while (!success) {
                try {
                    client.create(LOCK_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    success = true;
                } catch (KeeperException e) {
                    if (e.code() == KeeperException.Code.NODEEXISTS) {
                        synchronized (lock) {
                            client.exists(LOCK_PATH, this);
                            lock.wait();
                        }
                    }
                    e.printStackTrace();
                }
            }
            LOG.info(Thread.currentThread().getName() + "获取锁成功");
        }


        public boolean tryLock() {
            try {
                client.create(LOCK_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                return true;
            } catch (Exception e) {
                return false;
            }
        }


        public void unlock() throws Exception {
            client.delete(LOCK_PATH, 0);
            LOG.info(Thread.currentThread().getName() + "释放锁成功");
        }


        @Override
        public void process(WatchedEvent event) {
            if ((event.getType() == Event.EventType.NodeDeleted) && (LOCK_PATH.equals(event.getPath()))) {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }
    }

    /**
     * 竞争锁的线程,这里模拟加锁->业务操作->释放锁的流程
     */
    public static class FightThread extends Thread {

        public FightThread(String name) {
            super(name);
        }
        @Override
        public void run() {
            ZooKeeperLock lock;
            try {
              lock  = new ZooKeeperLock();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println("开始竞争锁");
            while (true) {
                try {
                    lock.lock();
                    Thread.sleep(2000);
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    try {
                        lock.unlock();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        }
    }

    public static void main(String[] args) throws Exception{

        FightThread t1 = new FightThread("线程-1");
        t1.setDaemon(true);
        FightThread t2 = new FightThread("线程-2");
        t2.setDaemon(true);
        t1.start();
        t2.start();
        Thread.sleep(60000);
    }
}
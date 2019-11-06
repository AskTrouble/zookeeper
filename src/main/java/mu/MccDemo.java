package mu;

import com.sun.rmi.rmid.ExecPermission;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 描述: 读取配置</br>
 *
 * @author yuas
 * @version 1.0.0
 * @since 2019/11/6 10:44
 * <p>
 * Copyright © 2019 BZN Corporation, All Rights Reserved.
 */
public class MccDemo {

    public static final String CONFIG_PATH = "/mcc/${yourAppKey}";

    public static class ConfigUtilAdapter implements Watcher {

        private ConcurrentHashMap<String, String> config = new ConcurrentHashMap<>();
        private static volatile boolean init = false;
        private ZooKeeper client;

        private static ConfigUtilAdapter instance;

        public static void init(String zkaddress){
            if(init){
                return;
            }
            instance = new ConfigUtilAdapter(zkaddress);
        }

        public ConfigUtilAdapter(String zkaddress){
            try {
                client = new ZooKeeper(zkaddress, 3000, this, false);
            } catch (IOException e){
                throw new IllegalStateException("初始化失败", e);
            }

            pullData();
        }

        private synchronized void pullData(){
            try {
                byte[] data = client.getData(CONFIG_PATH, this, null);
                Properties props = new Properties();
                props.load(new ByteArrayInputStream(data));
                config.clear();
                for(Map.Entry<Object, Object> entry : props.entrySet()){
                    config.put(entry.getKey().toString(), entry.getValue().toString());
                }
            } catch (Exception e){
                throw  new IllegalStateException("拉取配置失败" , e);
            }
        }

        public static String getString(String key) {
            return instance.config.get(key);
        }

        @Override
        public void process(WatchedEvent event){
            if((event.getType() == Event.EventType.NodeDataChanged) && (CONFIG_PATH.equals(event.getPath()))){
                pullData();
            }
        }

    }

    public static void main(String[] args) throws Exception{
        String zkaddress = "127.0.0.1:2181";
        mccInit(zkaddress);
        ConfigUtilAdapter.init(zkaddress);
        Thread.sleep(2000);

        String value = ConfigUtilAdapter.getString("key");

        System.out.println("第1次获取配置:" + value);
        mccCentralChange(zkaddress);
        Thread.sleep(500);
        value = ConfigUtilAdapter.getString("key");
        System.out.println("第2次获取配置:" + value);

    }

    public static void mccCentralChange(String zkaddress) throws Exception{
        try(ZooKeeper client = new ZooKeeper(zkaddress,  3000, even -> {}, false)){
            Properties props = new Properties();
            props.put("key", "helloworld");
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            props.store(out, null);
            client.setData( CONFIG_PATH, out.toByteArray(), 0);
        }
    }

    /**
     * 此初始化过程相当于服务申请时伴随appkey产生而执行的初始化操作
     *
     * @throws Exception
     */
    public static void mccInit(String zkaddress) throws Exception {
        try (ZooKeeper client = new ZooKeeper(zkaddress, 3000, event -> {
        }, false)) {

            try {
                Stat status = client.exists(CONFIG_PATH, false);
                if(status != null){
                    client.delete(CONFIG_PATH, status.getVersion());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                Stat status = client.exists("/mcc", false);
                if(status != null){
                    client.delete("/mcc", status.getVersion());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            client.create("/mcc", "mcc".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            client.create(CONFIG_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
}














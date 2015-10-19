package org.logsystems.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by youfuli on 10/12/15.
 */
public class ZooKeeperOperation implements Watcher{
    String hostPort;
    int sessionTimeout;
    ZooKeeper zooKeeper;
    String filename;
    public ZooKeeperOperation(String _hostPort, int _sessionTimeout, String filename) throws IOException {
        hostPort=_hostPort;
        sessionTimeout=_sessionTimeout;
        zooKeeper=new ZooKeeper(hostPort,sessionTimeout,this);
        this.filename=filename;
    }


    public void process(WatchedEvent event) {

    }

    public void write2ZooKeeper() throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(filename);
        int worksize=100;
        byte[] data="nihao".getBytes();
        for(int i=0;i<worksize;i++){
            try {
                fos.write(data);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String arg[]) throws IOException {
        String hostPort="";
        int timeout=0;
        String filename="";
        //ZooKeeperOperation zooKeeperOperation=new ZooKeeperOperation(hostPort,timeout,filename);
        //zooKeeperOperation.write2ZooKeeper();
    }
}

package org.logsystems.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by youfuli on 10/19/15.
 */
public class TestZkConnection {
    private static ZooKeeper zk;
    private static ZkConnector zkc;
    private static List<String> znodeList=new ArrayList<String>();
    public static void main(String[] args) throws IOException,InterruptedException,KeeperException{
        zkc=new ZkConnector();
        zk=zkc.connect("10.0.2.6");
        znodeList=zk.getChildren("/", true);
        for (String znode:znodeList){
            System.out.println(znode);
        }
    }
}

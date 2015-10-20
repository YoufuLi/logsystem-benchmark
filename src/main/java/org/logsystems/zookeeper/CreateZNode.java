package org.logsystems.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by youfuli on 10/19/15.
 */
public class CreateZNode {
    private static ZooKeeper zk;
    private static ZkConnector zkc;
    public static void create(String path, byte[] data) throws KeeperException, InterruptedException{
        zk.create(path,data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String path="/sampleznode";
        byte[] data="sample znode data".getBytes();
        zkc=new ZkConnector();
        zk=zkc.connect("localhost");
        create(path,data);
    }
}

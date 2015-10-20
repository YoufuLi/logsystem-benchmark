package org.logsystems.zookeeper;

import org.apache.zookeeper.*;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * Created by youfuli on 10/12/15.
 */
public class ZooKeeperOperation{
    private static ZooKeeper zk;
    private static ZkConnector zkc;

    public static void write2ZooKeeper(String path, byte[] data) throws FileNotFoundException, KeeperException, InterruptedException {
        zk.create(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public static void main(String arg[]) throws IOException, InterruptedException, KeeperException {
        long startT = System.currentTimeMillis();
        int worksize=10000;
        String path="/sampleznode/childnode";
        String tempPath="";
        zkc=new ZkConnector();
        zk=zkc.connect("localhost");
        for(int i=0;i<worksize;i++){
            tempPath=path+i;
            byte[] data="i".getBytes();
            write2ZooKeeper(tempPath, data);
        }
        long endT = System.currentTimeMillis();
        System.out.println(String.valueOf(endT - startT));
    }
}

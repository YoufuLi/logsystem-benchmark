package org.logsystems.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by youfuli on 10/19/15.
 */
public class DeleteZNode {
    private static ZooKeeper zk;
    private static ZkConnector zkc;
    private static List<String> znodeList=new ArrayList<String>();
    public static void delete(String path) throws KeeperException, InterruptedException{
        zk.delete(path,zk.exists(path,true).getVersion());
    }
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException{
        String path="/sampleznode/";
        zkc=new ZkConnector();
        zk=zkc.connect("localhost");
        //znodeList=zk.getChildren("/sampleznode", true);
//        for (String znode:znodeList){
//            delete(path+znode);
//        }
        for(int i=0;i<10000;i++){
            delete(path+"childnode"+i);
        }
    }
}

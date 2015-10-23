package org.logsystems.corfudb;

import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.stream.*;
import org.corfudb.runtime.view.*;
import org.corfudb.util.CorfuDBFactory;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by youfuli on 10/9/15.
 */
public class CorfuDBMultiple{
    private Logger log = LoggerFactory.getLogger(CorfuDBMultiple.class);
    Map<String,Object> opts=new HashMap<String, Object>();
    CorfuDBFactory cdbFactory;
    CorfuDBRuntime cdr;
    ICorfuDBInstance cinstance;
    public CorfuDBMultiple(String master,String serviceURL){
//        opts.put(master,serviceURL);
//        cdbFactory=new CorfuDBFactory(opts);
//        cdr=cdbFactory.getRuntime();
//        cinstance=cdr.getLocalInstance();
//        cinstance.getConfigurationMaster().resetAll();
        Map<String, Object> luConfigMap = new HashMap<String,Object>() {
            {
                put("capacity", 100000);
                put("ramdisk", true);
                put("pagesize", 4096);
                put("trim", 0);
            }
        };
        CorfuInfrastructureBuilder cbuilder = new CorfuInfrastructureBuilder().
                addLoggingUnit(7002, 0, NettyLogUnitServer.class, "nlu", luConfigMap).
                addLoggingUnit(7003, 0, NettyLogUnitServer.class, "nlu", luConfigMap).
                addLoggingUnit(7004, 0, NettyLogUnitServer.class, "nlu", luConfigMap).
                addSequencer(7001, NettyStreamingSequencerServer.class, "nsss", null).
                //              setReplication("cdbqr").
                        start(9999) ;

        cdr = CorfuDBRuntime.getRuntime("http://localhost:9999/corfu");

        cinstance = cdr.getLocalInstance();
        cinstance.getConfigurationMaster().resetAll();
    }

    public void appendData(UUID streamID, ICorfuDBInstance instance) throws IOException {
        //StreamingSequencer cfstreaming = new StreamingSequencer(client,streamID);
        //SimpleStream cfstream=new SimpleStream(streamID,instance);
        //SimpleLog cflog=new SimpleLog();
        IStream iStream = instance.openStream(streamID);
        //NewStream cfstream=new NewStream(streamID,instance);
        int worksize = 10000;
        //long nextPos=cfstreaming.getNext(streamID, worksize);
        //ITimestamp timestamp=cfstream.getFirstTimestamp();
        //System.out.println(cfstream.getFirstTimestamp());
        for (int j = 0; j < worksize; j++) {
            iStream.append("i".getBytes());
        }
//        int datacount=0;
//        int leftsize=worksize;
//        while (datacount<worksize){
//            for (int i = 0; i < leftsize; i++) {
//                try {
//                    iStream.append(i);
//                    datacount++;
//                    if(i==9999){
//                        leftsize=leftsize-i;
//                        i=0;
//                        instance.getConfigurationMaster().resetAll();
//                    }
//                } catch (IOException e) {
//                    System.out.println("nihao");
//                    e.printStackTrace();
//                }
//            }
//        }

    }

    public static void main(String args[]) throws InterruptedException, IOException {
        System.out.println("creating test thread...");
        CorfuDBMultiple cfmultiple=new CorfuDBMultiple("--master","http://localhost:8000/corfu");
        long startT = System.currentTimeMillis();
        UUID streamID=UUID.randomUUID();
        cfmultiple.appendData(streamID,cfmultiple.cinstance);
        long endT = System.currentTimeMillis();
        System.out.println("!!!!!!!!!!!!!!!All work completed!!!!!!!!!!!!!!!");
        System.out.println(String.valueOf(endT - startT));
    }
}

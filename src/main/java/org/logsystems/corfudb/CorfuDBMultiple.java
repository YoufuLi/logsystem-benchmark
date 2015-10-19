package org.logsystems.corfudb;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.*;
import org.corfudb.runtime.view.*;
import org.corfudb.util.CorfuDBFactory;
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
    private int nthreads = 2;
    public CorfuDBMultiple(String master,String serviceURL){
        opts.put(master,serviceURL);
        cdbFactory=new CorfuDBFactory(opts);
        cdr=cdbFactory.getRuntime();
        cinstance=cdr.getLocalInstance();
        cinstance.getConfigurationMaster().resetAll();
    }

    public void appendData(UUID streamID, ICorfuDBInstance instance) {
        //StreamingSequencer cfstreaming = new StreamingSequencer(client,streamID);
        SimpleStream cfstream=new SimpleStream(streamID,instance);
        int worksize = 1000000;
        //long nextPos=cfstreaming.getNext(streamID, worksize);
        //ITimestamp timestamp=cfstream.getFirstTimestamp();
        //System.out.println(cfstream.getFirstTimestamp());
        int datacount=0;
        int leftsize=worksize;
        while (datacount<worksize){
            for (int i = 0; i < leftsize; i++) {
                try {
                    ITimestamp timestamp=cfstream.append(i);
                    datacount++;
                    if(i==9999){
                        leftsize=leftsize-i;
                        i=0;
                        instance.getConfigurationMaster().resetAll();
                    }
                } catch (IOException e) {
                    System.out.println("nihao");
                    e.printStackTrace();
                }
            }
        }


    }

    public static void main(String args[]) throws InterruptedException {
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

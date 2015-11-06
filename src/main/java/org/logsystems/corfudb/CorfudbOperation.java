package org.logsystems.corfudb;

import org.corfudb.runtime.*;
import org.corfudb.runtime.stream.*;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.ISequencer;
import org.corfudb.runtime.view.IWriteOnceAddressSpace;
import org.corfudb.util.CorfuDBFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by youfuli on 10/8/15.
 */
public class CorfudbOperation {
    Map<String,Object> opts=new HashMap<String, Object>();
    CorfuDBFactory cdbFactory;
    CorfuDBRuntime cdr;
    ICorfuDBInstance cinstance;
    ILog cflog;
    public CorfudbOperation(String master,String serviceURL){
        opts.put(master,serviceURL);
        cdbFactory=new CorfuDBFactory(opts);
        cdr=cdbFactory.getRuntime();
        cinstance=cdr.getLocalInstance();
        cinstance.getConfigurationMaster().resetAll();
        ISequencer cfsequencer=cinstance.getSequencer();
        IWriteOnceAddressSpace cfwoas=cinstance.getAddressSpace();
        cflog=new SimpleLog(cfsequencer,cfwoas);
    }
    public ITimestamp append2CorfuDB() throws ClassNotFoundException {

        ITimestamp cftimestamp=new SimpleTimestamp(0);

        try {
            report("Begin:"+System.currentTimeMillis());
            //cflog.append((Serializable) cfmap);
            cflog.append("i");
            report("End:" + System.currentTimeMillis());

        } catch (OutOfSpaceException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
        return cftimestamp;
    }
    public void readFromCorfuDB(ITimestamp cftimestamp){
        try {
            Map<String, String> readmap;
            try {
                readmap = (Map<String, String>) cflog.read(cftimestamp);
                report(readmap.size());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }  catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]){
        CorfudbOperation cfoperation=new CorfudbOperation("--master","http://localhost:8000/corfu");
        ITimestamp cftimestamp = null;
//        Map<String,String> cfmap=new HashMap<>();
//        cfmap.put("No1","Hello CorfuDB");
//        cfmap.put("No2","Hello Youfu");
        try {
            cftimestamp=cfoperation.append2CorfuDB();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        cfoperation.readFromCorfuDB(cftimestamp);
    }
    public void report(Object message){
        System.out.println(message.toString());
    }
}

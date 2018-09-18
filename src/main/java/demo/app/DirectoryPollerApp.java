package demo.app;


import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;

import java.io.File;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

public class DirectoryPollerApp {

    private static final String DIRECTORY_POLLING_QUEUE = "pollingQueue";
    private static final String DIRECTORY_PATH = "d:/app/test/";
    private static final Long INTERVAL = 1000L;

    private static Ignite ignite = Ignition.start("config/data-node-config.xml");


    private static IgniteQueue<String> initializeQueue(){

        CollectionConfiguration colCfg = new CollectionConfiguration();
        colCfg.setCacheMode(PARTITIONED);

        return ignite.queue(DIRECTORY_POLLING_QUEUE, 0, colCfg);
    }

    private static FileAlterationMonitor initializeMonitor(IgniteQueue<String> queue){

        FileAlterationObserver observer = new FileAlterationObserver(DIRECTORY_PATH);
        FileAlterationMonitor monitor = new FileAlterationMonitor(INTERVAL);
        FileAlterationListener listener = new FileAlterationListenerAdaptor() {
            @Override
            public void onFileCreate(File file) {
                System.out.println("Create file with name: "+file.getName());
                System.out.println("Adding file to queue;");
                queue.offer(file.getName());
                System.out.println("Queue size: "+queue.size());
            }

            @Override
            public void onFileDelete(File file) {
                System.out.println("Delete file with name: "+file.getName());
            }

            @Override
            public void onFileChange(File file) {
            }
        };
        observer.addListener(listener);
        monitor.addObserver(observer);

        return monitor;
    }

    public static void main(String[] args) throws Exception {

        IgniteQueue<String> queue = initializeQueue();

        initializeMonitor(queue).start();
    }

}

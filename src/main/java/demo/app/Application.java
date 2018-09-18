package demo.app;


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;

import java.io.File;
import java.util.Objects;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

public class Application {

    private static final String DIRECTORY_POLLING_QUEUE = "pollingQueue";
    private static final String DIRECTORY_PATH = "d:/app/test/";
    private static final Long INTERVAL = 1500L;

    private static Ignite ignite = Ignition.start("config/client-node-config.xml");

    private static IgniteQueue<String> initializeQueue() {

        CollectionConfiguration colCfg = new CollectionConfiguration();
        colCfg.setCacheMode(PARTITIONED);

        return ignite.queue(DIRECTORY_POLLING_QUEUE, 0, colCfg);
    }

    public static void main(String[] args) throws InterruptedException {

        IgniteQueue<String> queue = initializeQueue();
        String fileName;

        while (true) {

            fileName = queue.poll();

            if(Objects.nonNull(fileName)) {
                try {
                    File file = new File(DIRECTORY_PATH +fileName);

                    if (file.delete()) {
                        System.out.println(file.getName() + " is deleted!");
                    } else {
                        System.out.println("Delete operation is failed.");
                        queue.offer(fileName);
                    }

                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    queue.offer(fileName);


                }
            } else
                System.out.println("Queue is empty!");
            Thread.sleep(INTERVAL);
        }
    }

}

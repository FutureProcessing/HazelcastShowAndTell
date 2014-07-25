import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;

public class HazelcastMapReducerCluster {

    public static void main(String[] args) throws IOException {
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        cfg.getManagementCenterConfig().setEnabled(true).setUrl("http://localhost:8080");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);

        Map<String, String> mapCustomers = instance.getMap("customers");
        mapCustomers.put("1", "Joe");
        mapCustomers.put("2", "Ali");
        mapCustomers.put("3", "Avi");
        System.out.println("Customer with key 1: " + mapCustomers.get("1"));
        System.out.println("Map Size:" + mapCustomers.size());

        Queue<String> queueCustomers = instance.getQueue("customers");
        queueCustomers.offer("Tom");
        queueCustomers.offer("Mary");
        queueCustomers.offer("Jane");
        System.out.println("First customer: " + queueCustomers.poll());
        System.out.println("Second customer: " + queueCustomers.peek());
        System.out.println("Queue size: " + queueCustomers.size());
    }
}

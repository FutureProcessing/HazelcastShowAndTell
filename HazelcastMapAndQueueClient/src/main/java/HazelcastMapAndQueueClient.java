import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.io.IOException;
import java.util.Queue;

public class HazelcastMapAndQueueClient {

    public static void main(String[] args) throws IOException {
        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap map = client.getMap("customers");
        System.out.println("Map Size:" + map.size());
        System.out.println("Map Element:" + map.get("2"));

        Queue<String> queueCustomers = client.getQueue("customers");
        System.out.println("Queue Size:" + queueCustomers.size());
        System.out.println("Queue Top:" + queueCustomers.poll());
    }
}

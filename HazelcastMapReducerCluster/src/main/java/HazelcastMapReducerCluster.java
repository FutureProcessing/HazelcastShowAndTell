import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;

public class HazelcastMapReducerCluster {

    public static void main(String[] args) throws IOException {
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
    }
}

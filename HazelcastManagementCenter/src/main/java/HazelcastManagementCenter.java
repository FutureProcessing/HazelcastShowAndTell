import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

public class HazelcastManagementCenter {

    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);

        WebAppContext webapp = new WebAppContext();
        webapp.setContextPath("/");
        webapp.setWar("src/main/resources/mancenter-3.2.4.war");

        server.setHandler(webapp);

        server.start();
        server.join();
    }
}
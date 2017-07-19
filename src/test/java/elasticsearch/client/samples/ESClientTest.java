package elasticsearch.client.samples;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ESClientTest {

    @Test
    public void testBasicSearch() throws UnknownHostException {
        final TransportClient client = getBasicESClient();
        final long id = System.currentTimeMillis();
        final Map<String, Object> source = putJsonDocument("adarsh", "thimmappa", new Date(), new String[] {"java"}, "11-12-1985", id+"");
        final IndexResponse indexResponse = client.prepareIndex("megacorp", "employee", ""+id).setSource(source).execute().actionGet();
    }

    private TransportClient getBasicESClient() throws UnknownHostException {
        final Settings settings = Settings.builder().put("cluster.name", "elasticsearch").put("xpack.security.user", "elastic:changeme").build();
        TransportClient transportClient = new PreBuiltXPackTransportClient(settings);
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        return transportClient;
    }

    public static Map<String, Object> putJsonDocument(String firstName, String lastName, Date joinedDate, String[] skills,
        String dob, String id) {

        Map<String, Object> jsonDocument = new HashMap<String, Object>();

        jsonDocument.put("firstName", firstName);
        jsonDocument.put("lastName", lastName);
        jsonDocument.put("joinedDate", joinedDate);
        jsonDocument.put("skills", skills);
        jsonDocument.put("dob", dob);
        jsonDocument.put("id", id);

        return jsonDocument;
    }
}

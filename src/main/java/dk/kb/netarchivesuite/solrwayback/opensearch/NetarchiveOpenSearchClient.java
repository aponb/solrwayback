package dk.kb.netarchivesuite.solrwayback.opensearch;

import dk.kb.netarchivesuite.solrwayback.solr.NetarchiveAbstractClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetarchiveOpenSearchClient extends NetarchiveAbstractClient {
    protected static final Logger log = LoggerFactory.getLogger(NetarchiveOpenSearchClient.class);

    protected static OpenSearchClient openSearchClient;
    protected static NetarchiveOpenSearchClient instance = null;

    public static final String OS_INDEXNAME_WARCDISCOVERY = "warcdiscovery";
    public static final int ROWS = 20;

    protected NetarchiveOpenSearchClient() { // private. Singleton
    }

    public static void initialize(String aOpenSearchServer) {

        openSearchClient = new OpenSearchClient("wc10", "admin", "admin", "https", 9200);

        instance = new NetarchiveOpenSearchClient();
        log.info("OpenSearchClient initialized with OpenSearch server url:" + aOpenSearchServer);
    }

    public static NetarchiveOpenSearchClient getInstance() {
        if (instance == null) {
            throw new IllegalArgumentException("OpenSearchClient not initialized");
        }
        return instance;
    }
}

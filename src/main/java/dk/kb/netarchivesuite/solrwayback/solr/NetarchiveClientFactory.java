package dk.kb.netarchivesuite.solrwayback.solr;

import dk.kb.netarchivesuite.solrwayback.opensearch.NetarchiveOpenSearchClient;
import dk.kb.netarchivesuite.solrwayback.properties.PropertiesLoader;

public class NetarchiveClientFactory {
    protected static NetarchiveAbstractClient instance = null;
	
	public static NetarchiveAbstractClient initializeClient() {
    	if ("opensearch".equals(PropertiesLoader.ENGINE_TYPE)) {
			NetarchiveOpenSearchClient.initialize(PropertiesLoader.OPENSEARCH_SERVER);
			instance = NetarchiveOpenSearchClient.getInstance();
		}
		else {
            NetarchiveSolrClient.initialize(PropertiesLoader.SOLR_SERVER);
			instance = NetarchiveSolrClient.getInstance();
		}
		
		return instance;
	}
	
    public static NetarchiveAbstractClient getInstance() {
        if (instance == null) {
            throw new IllegalArgumentException("NetarchiveClient not initialized");
        }
        return instance;
    }
}

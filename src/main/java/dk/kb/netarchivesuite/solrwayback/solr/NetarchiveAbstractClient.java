package dk.kb.netarchivesuite.solrwayback.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

public class NetarchiveAbstractClient {
    private static final Logger log = LoggerFactory.getLogger(NetarchiveAbstractClient.class);
    protected static final long M = 1000000; // ns -> ms

    protected static Pattern TAGS_VALID_PATTERN = Pattern.compile("[-_.a-zA-Z0-9Ã¦Ã¸Ã¥Ã†Ã˜Ã…]+");
    protected static String NO_REVISIT_FILTER ="record_type:response OR record_type:arc OR record_type:resource";

    protected static String indexDocFieldList = "id,score,title,url,url_norm,links_images,source_file_path,source_file,source_file_offset,domain,resourcename,content_type,content_type_full,content_type_norm,hash,type,crawl_date,content_encoding,exif_location,status_code,last_modified,redirect_to_norm";
    protected static String indexDocFieldListShort = "url,url_norm,source_file_path,source_file,source_file_offset,crawl_date";

}

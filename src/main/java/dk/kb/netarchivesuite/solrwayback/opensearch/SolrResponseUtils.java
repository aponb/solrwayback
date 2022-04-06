package dk.kb.netarchivesuite.solrwayback.opensearch;

import com.google.common.io.BaseEncoding;
import org.apache.lucene.search.Explanation;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.text.Text;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.opensearch.search.aggregations.metrics.TopHits;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/*
 * based on https://github.com/codelibs/elasticsearch-solr-api/blob/master/src/main/java/org/codelibs/elasticsearch/solr/solr/SolrResponseUtils.java
 */
public class SolrResponseUtils {
	protected static final Logger log = LoggerFactory.getLogger(SolrResponseUtils.class);

	public static final String FACET_FIELD_PREFIX = "facet_field_";
	public static final String FACET_QUERY_PREFIX = "facet_query_";

	private static final String CONTENT_TYPE_JSON = "application/json; charset=UTF-8";

	// regex and date format to detect ISO8601 date formats
	private static final Pattern ISO_DATE_PATTERN = Pattern
			.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?Z");

	private static final String YYYY_MM_DD_T_HH_MM_SS_Z = "yyyy-MM-dd'T'HH:mm:ss'Z'";

	private static final String YYYY_MM_DD_T_HH_MM_SS_SSS_Z = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

	private static final TimeZone TIMEZONE_UTC = TimeZone.getTimeZone("UTC");

	protected SolrResponseUtils() {
	}

	private static Date parseISODateFormat(final String value) {
		try {
			final SimpleDateFormat sdf = new SimpleDateFormat(YYYY_MM_DD_T_HH_MM_SS_SSS_Z, Locale.ROOT);
			sdf.setTimeZone(TIMEZONE_UTC);
			return sdf.parse(value);
		} catch (final ParseException e) {
			try {
				final SimpleDateFormat sdf = new SimpleDateFormat(YYYY_MM_DD_T_HH_MM_SS_Z, Locale.ROOT);
				sdf.setTimeZone(TIMEZONE_UTC);
				return sdf.parse(value);
			} catch (final ParseException e1) {
				throw new OpenSearchException("Could not parse " + value, e);
			}
		}
	}

	/**
	 * Converts the search response into a NamedList that the Solr Response Writer
	 * can use.
	 *
	 * @param request
	 *            the ES SearchRequest
	 * @param response
	 *            the ES SearchResponse
	 * @return a NamedList of the response
	 */
	public static NamedList<Object> createSearchResponse(final SearchRequest request, final SolrQuery solrQuery, final SearchResponse response) {
		final NamedList<Object> resp = new SimpleOrderedMap<Object>();
		final NamedList<Object> debugList = new SimpleOrderedMap<Object>();
		resp.add("responseHeader", createResponseHeader(request, solrQuery, response));
		
		//final NamedList<Object> grouping = createGroupedResponse(request, response);
		
		
		Aggregation groupingAggregation = null;
		
		if (response.getAggregations() != null) {
			Iterator<Aggregation> aggIter = response.getAggregations().iterator();
			while (aggIter.hasNext()) {
				Aggregation agg = aggIter.next();
				if (agg.getName().startsWith(AGGREGATION_GROUPED_PREFIX)) {
					groupingAggregation = agg;
					break;
				}
			}
		}
		
		if (groupingAggregation != null) {
			SolrDocumentList groupedDocuments = convertGroupingToSolrDocumentList((Terms)groupingAggregation); 

			final NamedList<Object> docListSection = new SimpleOrderedMap<Object>();
			docListSection.add("matches", response.getHits().getTotalHits().value);
			groupedDocuments.setNumFound(response.getHits().getTotalHits().value);
			docListSection.add("doclist", groupedDocuments);

			final NamedList<Object> groupedFieldSection = new SimpleOrderedMap<Object>();
			String groupedField = groupingAggregation.getName().substring(AGGREGATION_GROUPED_PREFIX.length());
			groupedFieldSection.add(groupedField, docListSection);
			
			resp.add("grouped", groupedFieldSection);
			
			final NamedList<Object> statsFieldsSection = new SimpleOrderedMap<Object>();
			final NamedList<Object> statsGroupedFieldSection = new SimpleOrderedMap<Object>();
			final NamedList<Object> cardinalitySection = new SimpleOrderedMap<Object>();

			int numberOfBuckers = ((Terms)groupingAggregation).getBuckets().size();
			
			cardinalitySection.add("cardinality", numberOfBuckers);
			statsGroupedFieldSection.add(groupedField, cardinalitySection);
			statsFieldsSection.add("stats_fields", statsGroupedFieldSection);
			resp.add("stats", statsFieldsSection);
		}
		else {
			resp.add("response", convertToSolrDocumentList(request, response, debugList));
		}

		// add highlight node if highlighting was requested
		final NamedList<Object> highlighting = createHighlightResponse(request, response);
		if (highlighting != null) {
			resp.add("highlighting", highlighting);
		}

		// add faceting node if faceting was requested
		final NamedList<Object> faceting = createFacetResponse(request, response);
		if (faceting != null) {
			resp.add("facet_counts", faceting);
		}

		if (debugList.size() > 0) {
			resp.add("debug", debugList);
		}

		return resp;
	}

	
	public static SolrDocumentList convertGroupingToSolrDocumentList(final Terms terms) {
		NamedList<Object> explainList = null;
		final SolrDocumentList results = new SolrDocumentList();

		List<? extends Bucket> buckets = terms.getBuckets();
		
		// set the result information on the SolrDocumentList
		//results.setMaxScore(buckets..getMaxScore());
		//results.setNumFound(hits.getTotalHits());
		//results.setStart(request.source().from());
		
		Iterator<? extends Bucket> it = buckets.iterator();
		while (it.hasNext()) {
			Bucket b = it.next();
			
			TopHits hits = b.getAggregations().get("top");
			
			// loop though the results and convert each
			// one to a SolrDocument
			for (final SearchHit hit : hits.getHits()) {
				final SolrDocument doc = new SolrDocument();

				// always add score to document
				doc.addField("score", hit.getScore());
				doc.addField("id", hit.getId());

				// attempt to get the returned fields
				// if none returned, use the source fields
				final Map<String, DocumentField> fields = hit.getFields();
				final Map<String, Object> source = hit.getSourceAsMap();
				if (fields.isEmpty()) {
					if (source != null) {
						for (final Map.Entry<String, Object> entry : source.entrySet()) {
							final String sourceField = entry.getKey();
							Object fieldValue = entry.getValue();

							// ES does not return date fields as Date Objects
							// detect if the string is a date, and if so
							// convert it to a Date object
							//if (fieldValue instanceof String && ISO_DATE_PATTERN.matcher(fieldValue.toString()).matches()) {
							//	fieldValue = parseISODateFormat(fieldValue.toString());
							//}

							doc.addField(sourceField, fieldValue);
						}
					}
				} else {
					for (final Map.Entry<String, DocumentField> entry : fields.entrySet()) {
						final String fieldName = entry.getKey();
						final DocumentField field = entry.getValue();
						Object fieldValue = field.getValue();

						// ES does not return date fields as Date Objects
						// detect if the string is a date, and if so
						// convert it to a Date object
						//if (fieldValue instanceof String && ISO_DATE_PATTERN.matcher(fieldValue.toString()).matches()) {
						//	fieldValue = parseISODateFormat(fieldValue.toString());
						//}

						doc.addField(fieldName, fieldValue);
					}
				}

				final Explanation explanation = hit.getExplanation();
				if (explanation != null) {
					if (explainList == null) {
						explainList = new SimpleOrderedMap<Object>();
					}
					explainList.add(hit.getId(), explanation.toString());
				}

				// add the SolrDocument to the SolrDocumentList
				results.add(doc);
			}
			
		}

		return results;
	}
	
	/**
	 * Creates the Solr response header based on the search response.
	 *
	 * @param request
	 *            the ES SearchRequest
	 * @param response
	 *            the ES SearchResponse
	 * @return the response header as a NamedList
	 */
	public static NamedList<Object> createResponseHeader(final SearchRequest request, final SolrQuery solrQuery, final SearchResponse response) {
		// generate response header
		final NamedList<Object> responseHeader = new SimpleOrderedMap<Object>();
		responseHeader.add("status", 0);
		responseHeader.add("QTime", (int) response.getTook().millis());

		// echo params in header
		final NamedList<Object> solrParams = new SimpleOrderedMap<Object>();
		for (final String key : solrQuery.getParameterNames()) {
			final String value = solrQuery.get(key);
			solrParams.add(key, value);
		}

		responseHeader.add("params", solrParams);

		return responseHeader;
	}

	/**
	 * Creates a NamedList for the for document highlighting response
	 *
	 * @param request
	 *            the ES RestRequest
	 * @param response
	 *            the ES SearchResponse
	 * @return a NamedList if highlighting was requested, null if not
	 */
	public static NamedList<Object> createHighlightResponse(final SearchRequest request, final SearchResponse response) {
		NamedList<Object> highlightResponse = null;

		// if highlighting was requested create the NamedList for the highlights
		if (request.source().highlighter() != null) {
			highlightResponse = new SimpleOrderedMap<Object>();
			final SearchHits hits = response.getHits();
			// for each hit, get each highlight field and put the list
			// of highlight fragments in a NamedList specific to the hit
			for (final SearchHit hit : hits.getHits()) {
				final NamedList<Object> docHighlights = new SimpleOrderedMap<Object>();
				final Map<String, HighlightField> highlightFields = hit.getHighlightFields();
				for (final Map.Entry<String, HighlightField> entry : highlightFields.entrySet()) {
					final String fieldName = entry.getKey();
					final HighlightField highlightField = entry.getValue();
					final Text[] fragments = highlightField.getFragments();
					final List<String> fragmentList = new ArrayList<String>(fragments.length);
					for (final Text fragment : fragments) {
						fragmentList.add(fragment.string());
					}
					docHighlights.add(fieldName, fragmentList.toArray(new String[fragmentList.size()]));
				}

				// highlighting by placing the doc highlights in the response
				// based on the document id
				highlightResponse.add(hit.getId(), docHighlights);
			}
		}

		// return the highlight response
		return highlightResponse;
	}

	public static final String AGGREGATION_FACET_PREFIX = "agg_facet_";

	public static NamedList<Object> createFacetResponse(final SearchRequest request, final SearchResponse response) {
		NamedList<Object> facetResponse = null;

		if (request.source().aggregations() != null) {
			facetResponse = new SimpleOrderedMap<Object>();

			// create NamedLists for field and query facets
			final NamedList<ArrayList<Object>> termFacets = new SimpleOrderedMap<ArrayList<Object>>();
			final NamedList<Object> queryFacets = new SimpleOrderedMap<Object>();

			// loop though all the facets populating the NamedLists we just
			// created
			final Iterator<Aggregation> facetIter = response.getAggregations().iterator();
			List<Tuple<String, Integer>> facetQueryList = null;
			ArrayList<Tuple<String, ArrayList<Object>>> facetTermList = null;
			while (facetIter.hasNext()) {
				final Aggregation facet = facetIter.next();
				if (facet.getName().startsWith(AGGREGATION_FACET_PREFIX)) {
					if (facetTermList == null) {
						facetTermList = new ArrayList<Tuple<String, ArrayList<Object>>>();
					}
					
					// we have term facet, create NamedList to store terms
					final Terms termFacet = (Terms) facet;
					ArrayList<Object> termFacetObj = new ArrayList<Object>();
					for (final Terms.Bucket tfEntry : termFacet.getBuckets()) {
						termFacetObj.add(tfEntry.getKeyAsString());
						termFacetObj.add(tfEntry.getDocCount());
					}

					String encodedField = facet.getName().substring(AGGREGATION_FACET_PREFIX.length());
					//termFacets.add(new String(BaseEncoding.base64().decode(encodedField), StandardCharsets.UTF_8), termFacetObj);
					facetTermList.add(new Tuple<String, ArrayList<Object>>(encodedField, termFacetObj));
				} else if (facet.getName().startsWith(FACET_QUERY_PREFIX)) {
					if (facetQueryList == null) {
						facetQueryList = new ArrayList<>();
					}
					final Filter queryFacet = (Filter) facet;
					String encodedQuery = queryFacet.getName().substring(FACET_QUERY_PREFIX.length());
					facetQueryList.add(new Tuple<String, Integer>(encodedQuery, (int) queryFacet.getDocCount()));
				}
			}

			if (facetQueryList != null) {
				Collections.sort(facetQueryList, new Comparator<Tuple<String, Integer>>() {
					@Override
					public int compare(Tuple<String, Integer> o1, Tuple<String, Integer> o2) {
						return o1.v1().compareTo(o2.v1());
					}
				});
				for (Tuple<String, Integer> tuple : facetQueryList) {
					int pos = tuple.v1().indexOf('_');
					queryFacets.add(new String(BaseEncoding.base64().decode(tuple.v1().substring(pos + 1)),
							StandardCharsets.UTF_8), tuple.v2());
				}
			}

			if (facetTermList != null) {
				/*
				Collections.sort(facetTermList, new Comparator<Tuple<String, ArrayList<Tuple<String, Integer>>>>() {
					@Override
					public int compare(Tuple<String, ArrayList<Tuple<String, Integer>>> o1, Tuple<String, ArrayList<Tuple<String, Integer>>> o2) {
						return o1.v1().compareTo(o2.v1());
					}
				});
				*/
			    for (Tuple<String, ArrayList<Object>> facet : facetTermList ) {
					termFacets.add(facet.v1(), facet.v2());
				}
			}
			
			facetResponse.add("facet_fields", termFacets);
			facetResponse.add("facet_queries", queryFacets);

			// add dummy facet_dates and facet_ranges since we dont support them
			// yet
			facetResponse.add("facet_dates", new SimpleOrderedMap<Object>());
			facetResponse.add("facet_ranges", new SimpleOrderedMap<Object>());

		}

		return facetResponse;
	}


	public static final String AGGREGATION_GROUPED_PREFIX = "agg_group_";
	
	public static NamedList<Object> createGroupedResponse(final SearchRequest request, final SearchResponse response) {
		NamedList<Object> groupResponse = null;

		if (request.source().aggregations() != null) {
			groupResponse = new SimpleOrderedMap<Object>();

			// create NamedLists for field and query facets
			//final NamedList<Object> termFacets = new SimpleOrderedMap<Object>();

			// loop though all the facets populating the NamedLists we just
			// created
			final Iterator<Aggregation> facetIter = response.getAggregations().iterator();
			while (facetIter.hasNext()) {
				final Aggregation facet = facetIter.next();
				if (facet.getName().startsWith(AGGREGATION_GROUPED_PREFIX)) {
					// we have term facet, create NamedList to store terms
					final Terms termFacet = (Terms) facet;
					final NamedList<Object> termFacetObj = new SimpleOrderedMap<Object>();
					for (final Terms.Bucket tfEntry : termFacet.getBuckets()) {
						termFacetObj.add(tfEntry.getKeyAsString(), (int) tfEntry.getDocCount());
					}

					String encodedField = facet.getName().substring(AGGREGATION_GROUPED_PREFIX.length());
					//termFacets.add(new String(BaseEncoding.base64().decode(encodedField), StandardCharsets.UTF_8), termFacetObj);
					groupResponse.add(encodedField, termFacetObj);
					
				}
			}
		}

		return groupResponse;
	}
	
	/**
	 * Converts the search results into a SolrDocumentList that can be serialized by
	 * the Solr Response Writer.
	 *
	 * @param request
	 *            the ES SearchRequest
	 * @param response
	 *            the ES SearchResponse
	 * @return search results as a SolrDocumentList
	 */
	public static SolrDocumentList convertToSolrDocumentList(final SearchRequest request, final SearchResponse response,
			final NamedList<Object> debugList) {
		NamedList<Object> explainList = null;
		final SolrDocumentList results = new SolrDocumentList();

		// get the ES hits
		final SearchHits hits = response.getHits();

		// set the result information on the SolrDocumentList
		results.setMaxScore(hits.getMaxScore());
		results.setNumFound(hits.getTotalHits().value);
		results.setStart(request.source().from());

		// loop though the results and convert each
		// one to a SolrDocument
		for (final SearchHit hit : hits.getHits()) {
			final SolrDocument doc = new SolrDocument();

			// always add score to document
			doc.addField("score", hit.getScore());
			doc.addField("id", hit.getId());

			// attempt to get the returned fields
			// if none returned, use the source fields
			final Map<String, DocumentField> fields = hit.getFields();
			final Map<String, Object> source = hit.getSourceAsMap();
			if (fields.isEmpty()) {
				if (source != null) {
					for (final Map.Entry<String, Object> entry : source.entrySet()) {
						final String sourceField = entry.getKey();
						Object fieldValue = entry.getValue();

						// ES does not return date fields as Date Objects
						// detect if the string is a date, and if so
						// convert it to a Date object
						//if (fieldValue instanceof String && ISO_DATE_PATTERN.matcher(fieldValue.toString()).matches()) {
						//	fieldValue = parseISODateFormat(fieldValue.toString());
						//}

						doc.addField(sourceField, fieldValue);
					}
				}
			} else {
				for (final Map.Entry<String, DocumentField> entry : fields.entrySet()) {
					final String fieldName = entry.getKey();
					final DocumentField field = entry.getValue();
					Object fieldValue = field.getValue();

					// ES does not return date fields as Date Objects
					// detect if the string is a date, and if so
					// convert it to a Date object
					//if (fieldValue instanceof String && ISO_DATE_PATTERN.matcher(fieldValue.toString()).matches()) {
					//	fieldValue = parseISODateFormat(fieldValue.toString());
					//}

					doc.addField(fieldName, fieldValue);
				}
			}

			final Explanation explanation = hit.getExplanation();
			if (explanation != null) {
				if (explainList == null) {
					explainList = new SimpleOrderedMap<Object>();
				}
				explainList.add(hit.getId(), explanation.toString());
			}

			// add the SolrDocument to the SolrDocumentList
			results.add(doc);
		}

		if (explainList != null) {
			debugList.add("explain", explainList);
		}

		return results;
	}

	public static String writeJsonResponse(final NamedList<Object> obj) {
		final Writer writer = new StringWriter();

		// try to serialize the data to xml
		try {
			JSONWriter jw = new JSONWriter(writer);
			jw.setNamedListStyle("");
			jw.write(obj);
			writer.close();
			return writer.toString();		
		} catch (Exception e) {
			log.error("Error writing JSON response", e);
			return null;
		}
	}
	
	
	public void main(String[] args) {
		SearchRequest request = null;
		SearchResponse response = null;
		NamedList<Object> resp = SolrResponseUtils.createFacetResponse(request, response);
	}
}

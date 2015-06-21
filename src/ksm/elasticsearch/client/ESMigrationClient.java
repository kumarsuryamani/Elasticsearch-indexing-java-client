/**
 * 
 */
package ksm.elasticsearch.client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;

import es.bayesian.link.ESBayesianRestCommunicator;


/**
 * @author suryamani
 *
 */
public class ESMigrationClient {
	
	private static final String BAYESIAN_PATTERN = "\"BayesianAnalysers\"";
	
	private static final String RSS_RECOGNITION_PATTERN1 = "\"feedname\"";
	private static final String RSS_RECOGNITION_PATTERN2 = "\"title\"";
	private static final String RSS_RECOGNITION_PATTERN3 = "\"publishDate\"";
	
	private static final String TWEET_RECOGNITION_PATTERN1 = "\"created_at\"";
	private static final String TWEET_RECOGNITION_PATTERN2 = "\"truncated\"";
	private static final String TWEET_RECOGNITION_PATTERN3 = "\"screen_name\"";

	private TransportClient client;
	private String analyserUri;
	private Character verbose;
	private Character type;
	private Character alreadyBayseian;
	
	public ESMigrationClient(String esIP, String analysisUri, Character alreadyBayseian, Character verbose, Character type) {
		client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(esIP,9300)));
		this.analyserUri = analysisUri;
		this.alreadyBayseian = alreadyBayseian;
		this.verbose = verbose;
		this.type = type;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		String esIP = null, analyserUri = null, verbose = null, type = null, alreadyBayesian = null;
		System.out.print("\n\n\t\tElasticsearch IP [default: localhost]: ");
		esIP = reader.readLine();
		if(esIP== null || esIP.trim().length()< 1) {
			esIP="localhost";
		}
        System.out.print("\n\n\t\tBayesian Analyser URI [default: http://31.222.176.25:8080/abayes/?query=]: ");
        analyserUri=reader.readLine();
        if(analyserUri == null || analyserUri.trim().length() < 1) {
        	analyserUri = "http://31.222.176.25:8080/abayes/?query=";
        }
        
        System.out.print("\n\n\t\tDo you want to also analyze documents which are already Bayesian analyzed Y/N [default: Y]: ");
        alreadyBayesian=reader.readLine();
        if(alreadyBayesian == null || alreadyBayesian.trim().length() < 1) {
        	alreadyBayesian = "Y";
        }
        
        System.out.print("\n\n\t\tDo you want to see the verbose output Y/N [default: N]: ");
        verbose=reader.readLine();
        if(verbose == null || verbose.trim().length() < 1) {
        	verbose = "N";
        }
        
        System.out.println("\n\t\tWhich documents you want to Re-Index... ");
        System.out.println("\n\t\tFor only RSS documents enter R");
        System.out.println("\t\tFor only Twitter documents enter T");
        System.out.println("\t\tFor all river indexed documents enter A");
        System.out.print("\n\n\t\tEnter your choice R|T|A [default: A]: ");
        type=reader.readLine();
        if(type == null || type.trim().length() < 1) {
        	type = "A";
        }
        
        
        reader.close();
        //esIP="127.0.0.1";
        //analyserUri = "http://31.222.176.25:8080/abayes/?query=";
        ESMigrationClient esClient = new ESMigrationClient(esIP, analyserUri, 
        		Character.toUpperCase(alreadyBayesian.trim().charAt(0)),
        		Character.toUpperCase(verbose.trim().charAt(0)),
        		Character.toUpperCase(type.trim().charAt(0)));
		esClient.reIndexRecord();
		esClient.client.close();
	}
	
	private void reIndexRecord() {
		//Check system health
		ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        
        if(clusterHealth.getStatus() == ClusterHealthStatus.GREEN) {
        	System.err.println("Cluster is unhealthy...exiting");
        	return ;
        }
		
		//scroll search for 1000 hits per shard

		SearchResponse scrollResp = client.prepareSearch()
		        .setSearchType(SearchType.SCAN)
		        .setScroll(new TimeValue(60000))
		        .setSize(1000).execute().actionGet(); 
		
		List<ESRiverRecord> records = new ArrayList<ESRiverRecord>();
		int totalHits = 0, bayesianAnalysedCnt = 0, rssIndexedCount = 0, twitterIndexedCount = 0;
		//Scroll until no hits are returned
		while (true) {
		    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
		    int tmpCnt = 0;
		    for (SearchHit sh : scrollResp.getHits()) {
		    	++totalHits;
		    	try{
		    		int bayesianIndex = -1;
			    	boolean isTweetDoc = false, isRSSDoc = false, isBayesianAnalysisRequired = true;
			    	
			    	String origSource = sh.getSourceAsString();
			    	
			    	if(isTwitterDocument(origSource)) {
			    		if(this.type.equals('R')) {
			    			continue;
			    		}
			    		isTweetDoc = true;
			    	}else if(isRSSDocument(origSource)) {
			    		if(this.type.equals('T')) {
			    			continue ;
			    		}
			    		isRSSDoc = true;
			    	}
			    	
			    	bayesianIndex = origSource.indexOf(BAYESIAN_PATTERN);
			    	
			    	if(isTweetDoc) {
			    		String tmp = origSource.substring(0,bayesianIndex);
			    		int length = tmp.length();
			    		if(tmp.charAt(length-1) == ','
			    				&& tmp.charAt(length -2) == '}') {
			    			origSource = origSource.substring(0,bayesianIndex-1);
			    		}
			    		
			    	}
			    	
			    	Map<String, String> data = getJSONasMap(origSource);
			    	isBayesianAnalysisRequired = isBayesianAnalysisRequired(data);
			    	if(bayesianIndex > 0 && !isBayesianAnalysisRequired && this.alreadyBayseian.equals('N')) {
			    		continue;
			    	}
			    	
			    	String baContent = null, updatedSrc = null;
			    	boolean isSrcUpdated = false;
			    	
			    	if(isRSSDoc) {
			    		Map<String,String> bayesianContent = fetchRSSResponse(data.get("title"), data.get("description"));
			    		List<Map<String,String>> bayesianContentList = new ArrayList<Map<String,String>>();
			    		bayesianContentList.add(bayesianContent);
			    		baContent = createBayesianJsonContent(bayesianContentList);
			    		baContent = baContent.substring(1, baContent.length());
			    		
			    		//updated source manipulation
			    		int cmaIndex = origSource.lastIndexOf(",");
			    		String rssContent = null;
			    		if(bayesianIndex > 0) {
			    			rssContent = origSource.substring(0, bayesianIndex);
			    		}else {
			    			rssContent= origSource.substring(0,cmaIndex+1);
			    		}
			    		
			    		StringBuilder sb = new StringBuilder();
			    		sb.append(rssContent).append(baContent).append(origSource.substring(cmaIndex));
			    		updatedSrc = sb.toString();
			    		isSrcUpdated = true;
			    		++rssIndexedCount;
			    	}
			    	
			    	if(isTweetDoc) {
			    		Map<String,String> bayesianContent = fetchTweetBayesianAnalysis(data.get("text"));
			    		List<Map<String,String>> bayesianContentList = new ArrayList<Map<String,String>>();
			    		bayesianContentList.add(bayesianContent);
			    		baContent = createBayesianJsonContent(bayesianContentList);
			    		baContent = baContent.substring(1, baContent.length());
			    		StringBuilder sb = new StringBuilder();
			    		sb.append(origSource.substring(0,origSource.length()-1)).append(',').append(baContent).append('}');
			    		updatedSrc = sb.toString();
			    		isSrcUpdated = true;
			    		++twitterIndexedCount;
			    	}
			    	
			    	if(isSrcUpdated) {
			    		ESRiverRecord record = new ESRiverRecord(sh.getIndex(), sh.getType(), sh.getId());
				    	record.setOriginalSource(origSource);
				    	record.setBayesianAnalysisNeeded(true);
				    	record.setUpdatedSource(updatedSrc);
				    	records.add(record);
			    		++bayesianAnalysedCnt;
			    		
			    		if(++tmpCnt == 10) {
			    			startReIndexer(records);
			    			tmpCnt=0;
			    		}
			    		
			    		if(this.verbose.equals('Y')) {
			    			System.out.println("-----------------------------------------------------------------------------");
			    			System.out.println("Bayesian anlysising the document ["+record+"]");
					    	System.out.println("Original source: "+origSource);
					    	System.out.println("Bayesian Content: "+baContent);
					    	System.out.println("Updated content: "+updatedSrc);
					    	System.out.println("-----------------------------**********--------------------------------------");
			    		}else {
			    			System.out.println("Bayesian anlysising the document ["+record+"]");
			    		}
			    	}
		    	}catch(Exception ex) {
		    		System.err.println("There is exception for record..ignore this and continue: "+ex.fillInStackTrace());
		    		continue;
		    	}
		    }
		    //Break condition: No hits are returned
		    if (scrollResp.hits().hits().length == 0) {
		    	if(records.size() > 0) {
		    		startReIndexer(records);
		    	}
		        break;
		    }
		}
		System.out.println("\t\t----------------------------Final Stats-----------------------------------");
		System.out.println("\n\n\t\t[ Total indexed items count= "+totalHits+" ]");
		System.out.println("\t\t[ Total Bayesian analysed needed records count= "+bayesianAnalysedCnt+" ]");
		if(this.type.equals('A') || this.type.equals('R')) {
			System.out.println("\t\t[ Total Bayesian analysed RSS documents count= "+rssIndexedCount+" ]");
		}
		if(this.type.equals('A') || this.type.equals('T')) {
			System.out.println("\t\t[ Total Bayesian analysed Twitter documents count= "+twitterIndexedCount+" ]");
		}
		System.out.println("\t\t---------------------------*****************------------------------------");
	}

	private boolean isRSSDocument(String src) {
		int firstOccurance = src.indexOf(RSS_RECOGNITION_PATTERN1);
		int secondOccurance = src.indexOf(RSS_RECOGNITION_PATTERN2);
		int thirdOccurance = src.indexOf(RSS_RECOGNITION_PATTERN3);
		
		if(firstOccurance > 0 && secondOccurance > 0 && thirdOccurance > 0 
				&& thirdOccurance > secondOccurance && secondOccurance > firstOccurance) {
			return true;
		}
		
		return false;
	}
	
	private boolean isTwitterDocument(String src) {
		int firstOccurance = src.indexOf(TWEET_RECOGNITION_PATTERN1);
		int secondOccurance = src.indexOf(TWEET_RECOGNITION_PATTERN2);
		int thirdOccurance = src.indexOf(TWEET_RECOGNITION_PATTERN3);
		
		if(firstOccurance > 0 && secondOccurance > 0 && thirdOccurance > 0 
				&& thirdOccurance > secondOccurance && secondOccurance > firstOccurance) {
			return true;
		}
		
		return false;
	}
	/**
	 * @param records
	 */
	private void startReIndexer(List<ESRiverRecord> records) {
		List<ESRiverRecord> tmpRecords = new ArrayList<ESRiverRecord>(records);
		ReindexerThread thread = new ReindexerThread(tmpRecords, client);
		new Thread(thread).start();
		records.clear();
	}
	
	private String createBayesianJsonContent(List<Map<String,String>> bayesianAnalyzedText) {
		XContentBuilder out = null;
		try {
			out = XContentFactory.jsonBuilder();
			if(bayesianAnalyzedText != null) {
		    	if(bayesianAnalyzedText.isEmpty()) {
		    		out.field("BayesianAnalysers", "{}");
		    	} else{
		    		out.startObject();
		    		out.startArray("BayesianAnalysers");
		    		out.startObject();
		    		for(Map<String, String> m : bayesianAnalyzedText) {
		    			if(m == null || m.isEmpty()) {
		    				continue;
		    			}
		    			for(Map.Entry<String, String> e : m.entrySet()) {
		    				out.startArray(e.getKey());
		    				out.startObject();
		        			out.field("text", e.getValue());
		        			out.endObject();
		        			out.endArray();
		    			}
		    		}
		    		out.endObject();
		    		out.endArray();
		    		out.endObject();
		    	}
		    }
			return out.string();
		} catch (IOException e1) {
			
			e1.printStackTrace();
		}
		return null;
	}
	
	/*
	sample data with Bayesian Analyser
	
	BayesianAnalysers.0.Subscribe.0.text  =  A
	BayesianAnalysers.0.Safety.0.text  =  B
	BayesianAnalysers.0.Desire.0.text  =  Undecided
	BayesianAnalysers.0.Intent.0.text  =  A 
	 */
	private boolean isBayesianAnalysisRequired(Map<String,String> data) {
		for(String key : data.keySet()) {
			if(key!= null && key.trim().startsWith("BayesianAnalysers") && key.trim().endsWith("text")) {
				return false;	
			}
		}
		return true;
	}
	
	
	private Map<String,String> getJSONasMap(String jsonSource) {
		JsonSettingsLoader loader = new JsonSettingsLoader();
		try {
			return loader.load(jsonSource);
			
		} catch (IOException e) {
			return null;
		}
	}
	
	/*
	 1.)  Fetch the Bayesian analysis response for indexed RSS and Twitter which need
	 2.) Update the current json source with Bayesian analyser
	 */
	
	private Map<String, String> fetchRSSResponse(String title, String description) {
		
		//typical RSS data currently stored
		//title  =  Drug price claims appalling - Hunt
		//description  =  Allegations that drug companies offered to collude with chemists to overcharge the NHS for certain drugs are "appalling", the health secretary says.
		return ESBayesianRestCommunicator.getRSSResponse(this.analyserUri, title, description);
	}
	
	private Map<String, String> fetchTweetBayesianAnalysis(String tweet) {
		return ESBayesianRestCommunicator.getTweetResponse(this.analyserUri, tweet);
	}
	
	private static void test1() {
		JsonSettingsLoader loader = new JsonSettingsLoader();
		try {
			BufferedReader br = new BufferedReader(new FileReader("test.txt"));
			
			StringBuilder sb = new StringBuilder();
			String tmp = null;
			while((tmp=br.readLine())!= null) {
				sb.append(tmp);
			}
			for(Map.Entry<String, String> e : loader.load(sb.toString()).entrySet()) {
				System.out.println(e.getKey()+" = "+e.getValue());
			}
			
		} catch (IOException e) {
			e.fillInStackTrace();
		}
	}
	
	private static class ReindexerThread implements Runnable {
		
		private List<ESRiverRecord> records;
		private TransportClient tClient;
		
		public ReindexerThread(List<ESRiverRecord> records, TransportClient tClient) {
			this.records = records;
			this.tClient = tClient;
		}

		@Override
		public void run() {
			if(records == null || records.isEmpty()) {
				System.out.println("\nThere is no river indexed data needs to be analysed with Bayesian service.\n\n");
				return ;
			}
			System.out.println("\n\n\t\t------------Re-indexing the "+records.size()+" items now..........");
			BulkRequestBuilder bulkDelete = tClient.prepareBulk();
			for(ESRiverRecord record : records) {
				bulkDelete.add(Requests.deleteRequest(record.getIndex()).type(record.getType()).id(record.getId()));
			}
			
			try {
				BulkResponse response = (BulkResponse) bulkDelete.execute().actionGet();
				if (response.hasFailures()) {
					BulkItemResponse[] itemResponseArr = response.items();
					for(BulkItemResponse itemResponse : itemResponseArr) {
						System.out.println("[ Failed to delete the item of  Index: "+itemResponse.getIndex()+",Type: "+itemResponse.getType()+",Id: "+itemResponse.getId()
								+",Reason: "+itemResponse.getFailureMessage()+" ]");
					}
				}else {
					System.out.println("\n\t -------------- Old indexed data deleted successfully.............\n\t");
				}
			} catch (Exception e) {
				System.err.println("Bulk delete for the indexed items.");
			}
			
			BulkRequestBuilder bulkIndexing = tClient.prepareBulk();
			for(ESRiverRecord record : records) {
				bulkIndexing.add(Requests.indexRequest(record.getIndex()).type(record.getType()).id(record.getId()).source(record.getUpdatedSource()));
			}
			
			try {
				BulkResponse response = (BulkResponse) bulkIndexing.execute().actionGet();
				if (response.hasFailures()) {
					BulkItemResponse[] itemResponseArr = response.items();
					for(BulkItemResponse itemResponse : itemResponseArr) {
						System.out.println("[ Failed to re-index item of Index: "+itemResponse.getIndex()+",Type: "+itemResponse.getType()+",Id: "+itemResponse.getId()
								+",Reason: "+itemResponse.getFailureMessage()+" ]");
					}
				}else{
					System.out.println("\n\t -------------- Re-indexed the river data with Bayesian analyzers successfully..............\n\t");
				}
			} catch (Exception e) {
				try {
					for(ESRiverRecord record : records) {
						IndexResponse response = tClient.prepareIndex(record.getIndex(),record.getType(), record.getId())
								.setSource(record.getUpdatedSource()).execute().actionGet();
					}
				}catch(Exception ex) {
					System.err.println("Bulk indexing failed for last batch Bayesian analysed contents.");
				}
			}
			
		}
		
	}
}

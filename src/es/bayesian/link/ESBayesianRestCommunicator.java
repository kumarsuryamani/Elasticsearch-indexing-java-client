package es.bayesian.link;

/**
 * @author suryamani
 *
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class ESBayesianRestCommunicator {
	
	private static final List<Character> ALPHA_NUM_LIST = new ArrayList<Character>();
	static {
		String alphanumerics = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
		for(char c : alphanumerics.toCharArray()) {
			ALPHA_NUM_LIST.add(c);
		}
	}
	
	public static Map<String, String> getTweetResponse(String analyserUri, String tweet) {
		String tweetData = null;
		try {
			tweetData = new String(tweet.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			tweetData = "";
			System.err.println("Unsupported coding for tweet data");
		}
		if(tweetData == null || tweetData.trim().length() < 1) {
			return null;
		}
		String data = processTextForBayesianAnalysis(removeUrlFromTweet(tweet));
		return fetchJsonResponse(URI.create(analyserUri), data);
	}
	
	public static Map<String, String> getRSSResponse(String analyserUri, String rssTitle, String rssDescription) {
		String rssDesStr = null;
		try {
			rssDesStr = new String(rssDescription.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			rssDesStr = "";
			System.err.println("UnsupportedCoding for RSS data: "+rssDescription);
		}
		
		String data = processTextForBayesianAnalysis(rssTitle)+" "+processTextForBayesianAnalysis(removeHTMLTagsFromDescription(rssDesStr));
		
		return fetchJsonResponse(URI.create(analyserUri), data);
	}
	
	private static Map<String, String> fetchJsonResponse(URI analyserUri, String data) {

		try {
			if (data == null || data.trim().length() <= 0) {
				return null;
			}
			
			String data1 = URLEncoder.encode(data, "UTF-8");
			StringBuilder reqUrl = new StringBuilder();
			reqUrl.append(analyserUri).append(data1);
			
			HttpGet request = new HttpGet(reqUrl.toString());
			HttpClient httpClient = new DefaultHttpClient();
			httpClient.getParams().setParameter("http.protocol.version", HttpVersion.HTTP_1_0);
			httpClient.getParams().setParameter("http.socket.timeout", new Integer(30000));
			httpClient.getParams().setParameter("http.protocol.content-charset", "UTF-8");
			request.addHeader("accept", "application/json");
			
			HttpResponse response = httpClient.execute(request);

			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				System.err.println("Bayesian response failed, error code : "+response.getStatusLine().getStatusCode()
						+", error message: "+response.getStatusLine().getReasonPhrase());
			}
			
			BufferedReader br = new BufferedReader(new InputStreamReader(
					(response.getEntity().getContent()), "UTF-8"));
			
			StringBuilder sb = new StringBuilder();
			String output;
			while ((output = br.readLine()) != null) {
				sb.append(output);
			}
			String content = sb.toString();
			content = content.replaceAll("(\\[|\\]|\")", "");
			String[] tmpArr = content.split("([\\}](\\s)*[,](\\s)*[\\{])");
			Map<String, String> bayesianResponse = new HashMap<String, String>();
			for(String s : tmpArr) {
				s=s.replaceAll("(\\{|\\})", "");
				String[] jsonRecords = s.split(",");
				if(jsonRecords != null && jsonRecords.length >= 2) {
					String analyzerName = jsonRecords[0].substring(indexOfData(jsonRecords[0])+1);
					String result = jsonRecords[1].substring(indexOfData(jsonRecords[1])+1);
					bayesianResponse.put(analyzerName, result);
				}
			}
			httpClient.getConnectionManager().shutdown();
					
			return bayesianResponse;
		} catch (MalformedURLException e) {
			System.err.println("Error 1: "+e.fillInStackTrace());
		} catch (IOException e) {
			System.err.println("Error 2: "+e.fillInStackTrace());
		}
		return null;
	}

	private static String removeHTMLTagsFromDescription(String description) {
		if(description == null || description.trim().length() < 1) {
			return "";
		}
		
		String tmp = description.replaceAll("\\<[^>]*>","");
		
		return tmp;
	}
	
	private static String removeUrlFromTweet(String tweet) {
		
        String commentstr1=tweet;
        String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
        Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(commentstr1);
        int i=0;
        while (m.find()) {
            commentstr1=commentstr1.replaceAll(m.group(i),"").trim();
            i++;
        }
        
       return commentstr1;
    }
	
	private static String processTextForBayesianAnalysis(String s) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < s.length(); ++i) {
			char ch = s.charAt(i);
			if(ALPHA_NUM_LIST.contains(Character.valueOf(ch))
					|| Character.isWhitespace(ch)) {
				sb.append(ch);
			}
		}
		return sb.toString();
	}
	
	private static int indexOfData(String data) {
		return data.indexOf(':');
	}
	
	
}

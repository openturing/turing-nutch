package com.viglet.turing.nutch.indexwriter;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.util.List;

public class TurNutchUtils {

	static CloudSolrClient getCloudSolrClient(List<String> urls) {
		CloudSolrClient sc = new CloudSolrClient.Builder(urls).withParallelUpdates(true).build();
		sc.connect();
		return sc;
	}

	static CloudSolrClient getCloudSolrClient(List<String> urls, String username, String password) {
		// Building http client
		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
		provider.setCredentials(AuthScope.ANY, credentials);

		HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();

		// Building the client
		CloudSolrClient sc = new CloudSolrClient.Builder(urls).withParallelUpdates(true).withHttpClient(client).build();
		sc.connect();
		return sc;
	}

	static SolrClient getHttpSolrClient(String url) {
		return new HttpSolrClient.Builder(url).build();
	}

	static String stripNonCharCodepoints(String input) {
		StringBuilder retval = new StringBuilder();
		char ch;

		for (int i = 0; i < input.length(); i++) {
			ch = input.charAt(i);

			// Strip all non-characters
			// http://unicode.org/cldr/utility/list-unicodeset.jsp?a=[:Noncharacter_Code_Point=True:]
			// and non-printable control characters except tabulator, new line and
			// carriage return
			if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
					ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
					(ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
					(ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {

				retval.append(ch);
			}
		}

		return retval.toString();
	}
}

package com.viglet.turing.nutch.indexwriter;

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexerMapReduce;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.viglet.turing.client.sn.job.TurSNJobAction;
import com.viglet.turing.client.sn.job.TurSNJobItem;
import com.viglet.turing.client.sn.job.TurSNJobItems;

public class TurNutchIndexWriter implements IndexWriter {

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private final TurSNJobItems turSNJobItems = new TurSNJobItems();
	private CloseableHttpClient client = HttpClients.createDefault();
	private ModifiableSolrParams params;

	private Configuration config;

	private String url;
	private String site;
	private boolean auth;

	private int batchSize;
	private int totalAdds = 0;
	private boolean delete = false;
	private String weightField;

	private String username;
	private String password;

	@Override
	public void delete(String key) throws IOException {
		final TurSNJobItem turSNJobItem = new TurSNJobItem();
		turSNJobItem.setTurSNJobAction(TurSNJobAction.DELETE);

		try {
			key = URLDecoder.decode(key, "UTF8");
		} catch (UnsupportedEncodingException e) {
			logger.error("Error decoding: " + key);
			throw new IOException("UnsupportedEncodingException for " + key);
		} catch (IllegalArgumentException e) {
			logger.warn("Could not decode: " + key + ", it probably wasn't encoded in the first place..");
		}

		// escape solr hash separator
		key = key.replaceAll("!", "\\!");

		if (delete) {
			Map<String, Object> attributes = new HashMap<String, Object>();
			attributes.put("id", key);
			turSNJobItem.setAttributes(attributes);
			turSNJobItems.add(turSNJobItem);
		}

		if (turSNJobItems.getTuringDocuments().size() >= batchSize) {
			push();
		}

	}

	@Override
	public void update(NutchDocument doc) throws IOException {
		write(doc);
	}

	@Override
	public void write(NutchDocument doc) throws IOException {
		final TurSNJobItem turSNJobItem = new TurSNJobItem();
		turSNJobItem.setTurSNJobAction(TurSNJobAction.CREATE);
		Map<String, Object> attributes = new HashMap<String, Object>();
		for (final Entry<String, NutchField> e : doc) {

			for (final Object val : e.getValue().getValues()) {
				// normalise the string representation for a Date
				Object val2 = val;

				if (val instanceof Date) {
					val2 = DateTimeFormatter.ISO_INSTANT.format(((Date) val).toInstant());
				}

				if (e.getKey().equals("content") || e.getKey().equals("title")) {
					val2 = stripNonCharCodepoints((String) val);
				}
				if (e.getKey().equals("content")) {
					attributes.put("text", val2);
				} else {
					attributes.put(e.getKey(), val2);
				}
			}
		}

		if (!weightField.isEmpty()) {
			attributes.put(weightField, doc.getWeight());
		}

		attributes.put("type", "Page");

		URL url = new URL(attributes.get("url").toString());
		String path[] = url.getPath().split("/");
		String date = null;
		if (path.length >= 4) {
			if (isNumeric(path[1]) && isNumeric(path[2]) && isNumeric(path[3])) {
				date = String.format("%s/%s/%s", path[1], path[2], path[3]);
			}
		}

		Date dt = new Date();
		if (date != null) {
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
			try {
				dt = dateFormat.parse(date);
			} catch (ParseException e1) {
				e1.printStackTrace();
			}
		}

		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
		df.setTimeZone(tz);
		attributes.put("displaydate", df.format(dt));
		turSNJobItem.setAttributes(attributes);
		turSNJobItems.add(turSNJobItem);
		totalAdds++;

		if (turSNJobItems.getTuringDocuments().size() >= batchSize) {
			push();
		}
	}

	@Override
	public void close() throws IOException {
		// Nothing
	}

	@Override
	public void commit() throws IOException {
		push();
	}

	private void push() throws IOException {
		if (turSNJobItems.getTuringDocuments().size() > 0) {
			int totalCreate = 0;
			int totalDelete = 0;

			for (TurSNJobItem turSNJobItem : turSNJobItems.getTuringDocuments()) {
				TurSNJobAction turSNJobAction = turSNJobItem.getTurSNJobAction();
				switch (turSNJobAction) {
				case CREATE:
					totalCreate++;
					break;
				case DELETE:
					totalDelete++;
					break;
				}
			}

			logger.info(String.format("Indexing %d/%d documents", totalCreate, totalAdds));
			logger.info(String.format("Deleting %d documents", totalDelete));

			boolean showOutput = false;
			String encoding = "UTF-8";

			ObjectMapper mapper = new ObjectMapper();
			String jsonResult = mapper.writeValueAsString(turSNJobItems);

			Charset utf8Charset = Charset.forName("UTF-8");
			Charset customCharset = Charset.forName(encoding);

			ByteBuffer inputBuffer = ByteBuffer.wrap(jsonResult.getBytes());

			// decode UTF-8
			CharBuffer data = utf8Charset.decode(inputBuffer);

			// encode
			ByteBuffer outputBuffer = customCharset.encode(data);

			byte[] outputData = new String(outputBuffer.array()).getBytes("UTF-8");
			String jsonUTF8 = new String(outputData);

			HttpPost httpPost = new HttpPost(String.format("%s/api/sn/%s/import", this.url, this.site));
			if (showOutput) {
				logger.info(jsonUTF8);
			}
			StringEntity entity = new StringEntity(new String(jsonUTF8), "UTF-8");
			httpPost.setEntity(entity);
			httpPost.setHeader("Accept", "application/json");
			httpPost.setHeader("Content-type", "application/json");
			httpPost.setHeader("Accept-Encoding", "UTF-8");

			if (this.auth) {
				basicAuth(httpPost);
			}

			@SuppressWarnings("unused")
			CloseableHttpResponse response = client.execute(httpPost);

			turSNJobItems.getTuringDocuments().clear();

		}
	}

	@Override
	public Configuration getConf() {
		return config;
	}

	@Override
	public void setConf(Configuration conf) {
		config = conf;
	}

	private void basicAuth(HttpPost httpPost) {
		if (this.username != null) {
			String auth = String.format("%s:%s", this.username, this.password);
			String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
			String authHeader = "Basic " + encodedAuth;
			httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
		}
	}

	private static boolean isNumeric(String str) {
		return str.matches("-?\\d+(\\.\\d+)?"); // match a number with optional '-' and decimal.
	}

	@Override
	public void open(JobConf job, String name) throws IOException {
		if (this.config.get("solr.server.url") != null) {
			String[] fullUrl = this.config.get("solr.server.url").split("/");
			this.site = fullUrl[fullUrl.length - 1];
			String[] partialUrl = Arrays.copyOf(fullUrl, fullUrl.length - 1);
			this.url = String.join("/", partialUrl);
		} else {
			this.url = this.config.get("turing.".concat(TurNutchConstants.SERVER_URL)) != null
					? this.config.get("turing.".concat(TurNutchConstants.SERVER_URL))
					: job.get(TurNutchConstants.SERVER_URL);

			this.site = this.config.get("turing.".concat(TurNutchConstants.SITE)) != null
					? this.config.get("turing.".concat(TurNutchConstants.SITE))
					: job.get(TurNutchConstants.SITE);
		}
		if (url == null) {
			String message = "Missing Turing URL.\n" + describe();
			logger.error(message);
			throw new RuntimeException(message);
		}
		this.auth = this.config.get("turing.".concat(TurNutchConstants.USE_AUTH)) != null
				? this.config.getBoolean("turing.".concat(TurNutchConstants.USE_AUTH), false)
				: job.getBoolean(TurNutchConstants.USE_AUTH, false);

		this.username = this.config.get("turing.".concat(TurNutchConstants.USERNAME)) != null
				? this.config.get("turing.".concat(TurNutchConstants.USERNAME))
				: job.get(TurNutchConstants.USERNAME);
		this.password = this.config.get("turing.".concat(TurNutchConstants.PASSWORD)) != null
				? this.config.get("turing.".concat(TurNutchConstants.PASSWORD))
				: job.get(TurNutchConstants.PASSWORD);

		init(job);
	}

	private void init(JobConf job) {
		delete = config.getBoolean(IndexerMapReduce.INDEXER_DELETE, false);
		batchSize = job.getInt(TurNutchConstants.COMMIT_SIZE, 1000);
		weightField = job.get(TurNutchConstants.WEIGHT_FIELD, "");
		// parse optional params
		params = new ModifiableSolrParams();
		String paramString = config.get(IndexerMapReduce.INDEXER_PARAMS);
		if (paramString != null) {
			String[] values = paramString.split("&");
			for (String v : values) {
				String[] kv = v.split("=");
				if (kv.length < 2) {
					continue;
				}
				params.add(kv[0], kv[1]);
			}
		}
	}

	@Override
	public String describe() {
		StringBuffer sb = new StringBuffer("TurNutchIndexWriter\n");
		sb.append("\t").append("Indexing to Viglet Turing Semantic Navigation");
		return sb.toString();
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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.viglet.turing.nutch.indexwriter;

import java.lang.invoke.MethodHandles;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.IndexerMapReduce;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.viglet.turing.api.sn.job.TurSNJobAction;
import com.viglet.turing.api.sn.job.TurSNJobItem;
import com.viglet.turing.api.sn.job.TurSNJobItems;

public class TurNutchIndexWriter implements IndexWriter {

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private ModifiableSolrParams params;

	private Configuration config;

	private final TurSNJobItems turSNJobItems = new TurSNJobItems();

	private CloseableHttpClient client = HttpClients.createDefault();

	private int batchSize;
	private int totalAdds = 0;
	private boolean delete = false;
	private String weightField;

	@Override
	public void open(Configuration conf, String name) {
		// Implementation not required
	}

	/**
	 * Initializes the internal variables from a given index writer configuration.
	 *
	 * @param parameters Params from the index writer configuration.
	 */
	@Override
	public void open(IndexWriterParams parameters) {

		String[] urls = parameters.getStrings(TurNutchConstants.SERVER_URLS);

		if (urls == null) {
			String message = "Missing SOLR URL.\n" + describe();
			logger.error(message);
			throw new RuntimeException(message);
		}

		init(parameters);
	}

	private void init(IndexWriterParams properties) {
		batchSize = properties.getInt(TurNutchConstants.COMMIT_SIZE, 1000);
		delete = config.getBoolean(IndexerMapReduce.INDEXER_DELETE, false);
		weightField = properties.get(TurNutchConstants.WEIGHT_FIELD, "");

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
					val2 = TurNutchUtils.stripNonCharCodepoints((String) val);
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

		String embed = "http://sample.com/wp-json/oembed/1.0/embed?url=";

		String jsonURL = String.format("%s%s&format=json", embed,
				URLEncoder.encode(attributes.get("url").toString(), "UTF-8"));
		InputStream is = null;
		try {
			is = new URL(jsonURL).openStream();
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
			if (json.has("author_name"))
				attributes.put("author", json.get("author_name").toString());

			if (json.has("thumbnail_url"))
				attributes.put("image", json.get("thumbnail_url").toString());

		} catch (JSONException e1) {
			System.out.println(jsonURL);
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (FileNotFoundException fnfe) {
			// continue
		} finally {
			if (is != null) {
				is.close();
			}
		}

		turSNJobItem.setAttributes(attributes);
		turSNJobItems.add(turSNJobItem);
		totalAdds++;

		if (turSNJobItems.getTuringDocuments().size() >= batchSize) {
			push();
		}
	}

	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	public static boolean isNumeric(String str) {
		return str.matches("-?\\d+(\\.\\d+)?"); // match a number with optional '-' and decimal.
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

			String turingServer = "http://localhost:2700";
			String site = "Sample Site";
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

			HttpPost httpPost = new HttpPost(String.format("%s/api/sn/%s/import", turingServer, site));
			if (showOutput) {
				logger.info(jsonUTF8);
			}
			StringEntity entity = new StringEntity(new String(jsonUTF8), "UTF-8");
			httpPost.setEntity(entity);
			httpPost.setHeader("Accept", "application/json");
			httpPost.setHeader("Content-type", "application/json");
			httpPost.setHeader("Accept-Encoding", "UTF-8");

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

	/**
	 * Returns a String describing the IndexWriter instance and the specific
	 * parameters it can take.
	 *
	 * @return The full description.
	 */
	@Override
	public String describe() {
		StringBuffer sb = new StringBuffer("TurNutchIndexWriter\n");
		sb.append("\t").append("Indexing to Viglet Turing Semantic Navigation");
		return sb.toString();
	}

	@Override
	public void close() throws IOException {
		client.close();
	}
}
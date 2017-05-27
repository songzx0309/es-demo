import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.elasticsearch.xpack.security.authc.support.SecuredString;

import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

import org.junit.Test;

import com.gome.els.model.User;


public class AppTest {
	public TransportClient client;
	
	public AppTest() {
		try {
			client = new PreBuiltXPackTransportClient(Settings.builder()
			        .put("cluster.name", "docker-cluster")
			        .put("xpack.security.user", "elastic:changeme")
			        .build())
			    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node1"), 9300));
//			String token = basicAuthHeaderValue("elastic", new SecuredString("changeme".toCharArray()));
//			client.filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch().get();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testInit() throws Exception {
		XContentBuilder builder = jsonBuilder().startObject()
				.field("name", "lilei")
				.field("birth", "1980-05-15")
				.endObject();
		client.prepareIndex("demo", "user", "1").setSource(builder).get();
		
		builder = jsonBuilder().startObject()
				.field("name", "hanmeimei")
				.field("birth", "1981-06-01")
				.endObject();
		client.prepareIndex("demo", "user", "2").setSource(builder).get();
		
		builder = jsonBuilder().startObject()
				.field("name", "lucy")
				.field("birth", "1981-03-21")
				.endObject();
		client.prepareIndex("demo", "user", "3").setSource(builder).get();
		
		builder = jsonBuilder().startObject()
				.field("name", "lily")
				.field("birth", "1981-03-21")
				.endObject();
		client.prepareIndex("demo", "user", "4").setSource(builder).get();
		
		builder = jsonBuilder().startObject()
				.field("name", "kite")
				.field("birth", "1983-04-15")
				.endObject();
		client.prepareIndex("demo", "user", "5").setSource(builder).get();
	}
	
	@Test
	public void testAuth() throws Exception {
		Settings settings = Settings.builder()
				.put("cluster.name", "docker-cluster")
				.put("xpack.security.user", "elastic:changeme")
				.build();
		client = new PreBuiltXPackTransportClient(settings)
		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.144.36.186"),9300));
		System.out.println(client);
	}
	
	@Test
	public void testSimple() throws Exception {
		System.out.println(client);
	}
	
	@Test
	public void testIndex1() throws Exception {
		XContentBuilder builder = jsonBuilder().startObject()
				.field("name", "lilei")
				.field("birth", "1980-05-15")
				.endObject();

		String json = builder.string();
		System.out.println(json);
		IndexResponse response = client.prepareIndex("demo", "user", "4").setSource(builder).get();
		Result result = response.getResult();
		System.out.println(result.toString());
	}
	
	@Test
	public void testIndex2() throws Exception {
		User user = new User();
		user.setName("hanmeimei");
		user.setBirth("1981-02-15");
		com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
		byte[] json = mapper.writeValueAsBytes(user);
		System.out.println(new String(json));
		IndexRequestBuilder request = client.prepareIndex("demo", "user", "5").setSource(json);
		IndexResponse response = request.get();
		Result result = response.getResult();
		System.out.println(result.toString());
	}
	
	@Test
	public void testGet() throws Exception {
		GetRequestBuilder request = client.prepareGet("demo", "user", "3");
		GetResponse response = request.get();
		System.out.println(response);
	}
	
	@Test
	public void testMultiGet() throws Exception {
		MultiGetResponse itemResponses = client.prepareMultiGet()
				.add("demo", "user", "2")
				.add("demo", "clazz", "1")
				.add("demo", "user", "2" , "3", "5")
				.get();
		for(MultiGetItemResponse itemResponse : itemResponses) {
			GetResponse response = itemResponse.getResponse();
			System.out.println(response);
		}
	}
	
	@Test
	public void testDelete() throws Exception {
		DeleteRequestBuilder request = client.prepareDelete("demo", "user", "1");
		DeleteResponse response = request.get();
		System.out.println(response);
	}
	
	@Test
	public void testUpdate1() throws Exception {
		User user = new User();
		user.setName("smith");
		user.setBirth("1990-04-18");
		com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
		byte[] json = mapper.writeValueAsBytes(user);
		UpdateRequest updateRequest = new UpdateRequest("demo", "user", "3");
		updateRequest.doc(json);
		UpdateResponse response = client.update(updateRequest).get();
		System.out.println(response);
	}
	
	@Test
	public void testUpdate2() throws Exception {
		UpdateRequest updateRequest = new UpdateRequest("demo", "user", "3")
			.doc(jsonBuilder().startObject()
					.field("name", "json")
					.field("birth", "1990-04-13")
					.endObject());
		UpdateResponse response = client.update(updateRequest).get();
		System.out.println(response);
	}
	
	@Test
	public void testPartialUpdate() throws Exception {
		UpdateRequest updateRequest = new UpdateRequest("demo", "user", "3")
			.doc(jsonBuilder().startObject().field("birth","1990-06-18").endObject());
		UpdateResponse response = client.update(updateRequest).get();
		System.out.println(response);
	}
	
	@Test
	public void testSimpleSearch() throws Exception {
		SearchResponse response = client.prepareSearch().get();
		System.out.println(response);
	}
	
	@Test
	public void testSearch() throws Exception {
		SearchResponse response = client.prepareSearch("demo")
				.setTypes("user")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.termsQuery("name", "li", "kite"))
				.get();
		System.out.println(response);
	}
	
	@Test
	public void testWildSearch() throws Exception {
		SearchResponse response = client.prepareSearch("order")
				.setTypes("online")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.termsQuery("orderId", "19103185058"))
				.get();
		System.out.println(response);
	}
	
}

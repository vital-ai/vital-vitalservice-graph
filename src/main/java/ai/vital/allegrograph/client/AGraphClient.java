package ai.vital.allegrograph.client;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONParser;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.nquads.NQuadsParser;
import org.openrdf.rio.nquads.NQuadsWriter;
import org.openrdf.rio.ntriples.NTriplesWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.properties.Property_hasProvenance;
import ai.vital.vitalsigns.rdf.RDFFormat;
import ai.vital.vitalsigns.rdf.VitalNTripleIterator;

/**
 * Allegrograph REST API Java Client interface
 * Based on <a href="http://franz.com/agraph/support/documentation/4.14.1/http-reference.html">4.14.1 HTTP Reference</a>
 *
 */
public class AGraphClient {
	
	private AGraphClientConfig config;

	private HttpClient httpClient;
	
	private ObjectMapper mapper = new ObjectMapper();
	
	private final static Logger log = LoggerFactory.getLogger(AGraphClient.class);
	
	public AGraphClient(AGraphClientConfig config) {
		this.config = config;
		
		httpClient = new HttpClient();
		Integer connectionsCount = config.getConnectionsCount();
		if(connectionsCount == null || connectionsCount.intValue() <= 0) {
			//default
			connectionsCount = MultiThreadedHttpConnectionManager.DEFAULT_MAX_TOTAL_CONNECTIONS;
			log.warn("Using default connections pool size: {}", connectionsCount);
		} else {
			log.info("Connections pool size: {}", connectionsCount);
		}
		if(connectionsCount > 1) {
			MultiThreadedHttpConnectionManager multiThreadedHttpConnectionManager = new MultiThreadedHttpConnectionManager();
			multiThreadedHttpConnectionManager.getParams().setMaxTotalConnections(connectionsCount);
			multiThreadedHttpConnectionManager.getParams().setDefaultMaxConnectionsPerHost(connectionsCount);
			httpClient.setHttpConnectionManager(multiThreadedHttpConnectionManager);
		} else {
			//single connection pool
		}
		httpClient.getParams().setAuthenticationPreemptive(true);

		if(config.getUsername() != null && !config.getUsername().isEmpty() 
				&& config.getPassword() != null && !config.getPassword().isEmpty()) {
			Credentials defaultcreds = new UsernamePasswordCredentials(config.getUsername(), config.getPassword());
			httpClient.getState().setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM), defaultcreds);
		}
	}

	public AGraphClientConfig getConfig() {
		return config;
	}
	
	private String getRepositoryURL() {
		return config.getServerURL() +
				(config.getCatalogName() != null && !config.getCatalogName().isEmpty() ? (config.getCatalogName() + "/") : "" ) +
				"repositories/" + config.getRepositoryName();
	}
	
	private String getCatalogURLSlash() {
		return config.getServerURL() +
				(config.getCatalogName() != null && !config.getCatalogName().isEmpty() ? (config.getCatalogName() + "/") : "" ); 
	}
	
	/**
	 * Executes sparql select and returns simplified json results
	 * @param sparql
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public LinkedHashMap<String, Object> sparqlSelectSimpleJsonOutput(String sparql) throws Exception {
		
		PostMethod method = new PostMethod(getRepositoryURL());

		InputStream stream = null;
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			method.addRequestHeader("Accept", "application/json");
			method.setRequestBody(new NameValuePair[]{ new NameValuePair("query", sparql) });
			
			execute(method);
			
			stream = method.getResponseBodyAsStream();
			
			return mapper.readValue(stream, LinkedHashMap.class);
			
		} finally {
			
			IOUtils.closeQuietly(stream);
			method.releaseConnection();
			
		}
		
	}
	
	/**
	 * Executes sparql select and returns
	 * @param sparql
	 * @return
	 * @throws Exception
	 */
	public void sparqlSelectJsonOutput(String sparql, SPARQLResultsJSONParser parser) throws Exception {
		
		PostMethod method = new PostMethod(getRepositoryURL());
		
		InputStream stream = null;
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			method.addRequestHeader("Accept", "application/sparql-results+json");
			method.setRequestBody(new NameValuePair[]{ new NameValuePair("query", sparql) });
			
			execute(method);
			
			stream = method.getResponseBodyAsStream();
			
			parser.parseQueryResult(stream);
			
		} finally {
			
			IOUtils.closeQuietly(stream);
			method.releaseConnection();
			
		}
		
	}

	private void execute(HttpMethodBase method) throws Exception {
		
		int status = httpClient.executeMethod(method);
		
		if(status < 200 || status > 299) {
			String statusLine = "";
			String err = "";
			try {
				err = method.getResponseBodyAsString(2048);
			} catch(Exception e) {}
			try {
				statusLine = method.getStatusText();
			} catch(Exception e) {}
			throw new Exception("AGRAPH HTTP status: " + status + " - " + statusLine + "  - error message: " + err);
		}
		
	}

	public void deleteRepository() throws Exception {

		DeleteMethod method = new DeleteMethod(getRepositoryURL());
		
		try {
			
			execute(method);
			
		} finally {
			
			method.releaseConnection();
			
		}
	
	}

	public boolean createRepository(boolean override) throws Exception {

		
		// Creates a new, empty repository. Supports several optional configuration arguments:
		// expectedSize
		//    An integer, used to configure the expected size of the repository.
		// index
		//    Can be specified any number of times. Should hold index IDs, and is used to configure the set of indices created for the store. 
		//
		// When a repository with the given name already exists, it is overwritten, unless a parameter override with value false is passed. 
		
		if(!override) {
			//check if already exists
			GetMethod method = new GetMethod(getCatalogURLSlash() + "repositories");
			method.setRequestHeader("Accept", "application/json");
			InputStream stream = null;
			try {
				
				execute(method);
				
				stream = method.getResponseBodyAsStream();
				
				@SuppressWarnings("unchecked")
				List<LinkedHashMap<String, Object>> repositories = mapper.readValue(stream, List.class);
				
				for(LinkedHashMap<String, Object> repo : repositories) {
					
					if(config.getRepositoryName().equals( repo.get("id") ) ) {
						return false;
					}
					
				}
				
				
			} finally {
				IOUtils.closeQuietly(stream);
				method.releaseConnection();
			}
			
		}
		
		PutMethod method = new PutMethod(getRepositoryURL());

		try {
			
			// method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			method.setRequestEntity(toForm(new NameValuePair[]{ new NameValuePair("override", "" + override) }));
			
			execute(method);
			
			return true;
			
		} finally {
			
			method.releaseConnection();
			
		}
				
	}

	private RequestEntity toForm(NameValuePair[] nameValuePairs) throws Exception {

		StringBuilder sb = new StringBuilder();
		
		for(NameValuePair p : nameValuePairs) {
			if(sb.length() > 0) sb.append('&');
			sb.append(URLEncoder.encode(p.getName(), "UTF-8")).append('=').append(URLEncoder.encode(p.getValue(), "UTF-8"));
		}
		
		StringRequestEntity ent = new StringRequestEntity(sb.toString(), "application/x-www-form-urlencoded", "UTF-8");
		
		return ent;
	}
	
	private String toFormString(Map<String, String> params) throws Exception {
		StringBuilder sb = new StringBuilder();
		for(Entry<String, String> e : params.entrySet()) {
			if(sb.length() > 0) sb.append('&');
			sb.append(es(e.getKey())).append('=').append(es(e.getValue()));
		}
		return sb.toString();
	}

	public void addTriples(AGraphSession session, String graphURI, RequestEntity requestEntity) throws Exception {
		
		PostMethod method = new PostMethod( ( session != null ? session.getSessionURL() : getRepositoryURL() ) + "/statements?context=" + es("<" + graphURI + ">"));
		
		try {
			
			method.setRequestEntity(requestEntity);
			
			execute(method);
			
		} finally {
			method.releaseConnection();
		}
	
	}
	
	public void addNTriples(String graphURI, InputStream inputStream) throws Exception {

		PostMethod method = new PostMethod(getRepositoryURL() + "/statements?context=" + es("<" + graphURI + ">"));
		
		try {
			
			method.addRequestHeader("Content-Type", "text/plain");
			method.setRequestEntity(new InputStreamRequestEntity(inputStream));
			
			execute(method);
			
		} finally {
			method.releaseConnection();
		}
		
	}

	private String es(String s) throws Exception{
		return URLEncoder.encode(s, "UTF-8");
	}

	public String getVersion() throws Exception {

		GetMethod method = new GetMethod(config.getServerURL() + "version");
		
		try {

			execute(method);
			
			return method.getResponseBodyAsString(2048);
			
		} finally {
			method.releaseConnection();
		}
		
	}

	public int deleteGraph(AGraphSession currentSession, String sid) throws Exception {

		DeleteMethod method = new DeleteMethod( ( currentSession != null ? currentSession.getSessionURL() : getRepositoryURL() ) + "/statements?context=" + es("<" + sid + ">"));  

		try {
			
			execute(method);
			
			String responseBodyAsString = method.getResponseBodyAsString(2048);
			
			return Integer.parseInt(responseBodyAsString);
			
		} finally {
			method.releaseConnection();
		}
		
	}

	public void sparqlGraphNQuadsOutput(String sparql, NQuadsParser parser, boolean expectQuadsNotTriples) throws Exception {

		
		PostMethod method = new PostMethod(getRepositoryURL());
		
		InputStream stream = null;
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			method.addRequestHeader("Accept", expectQuadsNotTriples ? "text/x-nquads" : "text/plain");
			method.setRequestBody(new NameValuePair[]{ new NameValuePair("query", sparql) });
			
			execute(method);
			
			stream = method.getResponseBodyAsStream();
			
			parser.parse(stream, "");
			
		} finally {
			
			IOUtils.closeQuietly(stream);
			method.releaseConnection();
			
		}
		
	}

	public boolean sparqlAskQuery(String sparql) throws Exception {
		
		PostMethod method = new PostMethod(getRepositoryURL());
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			method.addRequestHeader("Accept", "text/plain");
			method.setRequestBody(new NameValuePair[]{ new NameValuePair("query", sparql) });
			
			execute(method);

			String string = method.getResponseBodyAsString(16384);
			
			return Boolean.parseBoolean(string);
			
		} finally {
			
			method.releaseConnection();
			
		}
	}

	public int addStatements(AGraphSession currentSession, List<Statement> statements) throws Exception {
		
		PostMethod method = new PostMethod((currentSession != null ? currentSession.getSessionURL() : getRepositoryURL()) + "/statements");
		
		try {
			method.addRequestHeader("Accept", "*/*");
			
			// method.addRequestHeader("Content-Type", "text/plain");
			
			method.setRequestEntity(new QuadsStatementsRequestEntity(statements));
			
			execute(method);
			
			return Integer.parseInt(method.getResponseBodyAsString(2048));
			
		} finally {
			method.releaseConnection();
		}
		
	}
	
	public void deleteStatements(AGraphSession currentSession, List<Statement> stmts) throws Exception {

		if(stmts.size() < 1) return;
		
		String sparql = "DELETE DATA {\n";
		
		Map<String, List<Statement>> context2Stmts = new LinkedHashMap<String, List<Statement>>();
		
		// group statements by graph context
		for(Statement stmt : stmts) {
			String ctx = null;
			if(stmt instanceof ContextStatementImpl) {
				ContextStatementImpl cstmt = (ContextStatementImpl) stmt;
				ctx = cstmt.getContext().stringValue();
			} else {
				ctx = "";
			}
			List<Statement> list = context2Stmts.get(ctx);
			if(list == null) {
				list = new ArrayList<Statement>();
				context2Stmts.put(ctx, list);
			}
			list.add(stmt);
		}
		
		if(context2Stmts.containsKey("") && context2Stmts.size() > 1) throw new RuntimeException("Cannot delete both named graph and default graph data at once!");
		
		for( Entry<String, List<Statement>> entry : context2Stmts.entrySet()) {
			String gid = entry.getKey();
			
			StringWriter sw = new StringWriter();
			NTriplesWriter writer = new NTriplesWriter(sw);
			writer.startRDF();
			for(Statement stmt : entry.getValue() ) {
				writer.handleStatement(stmt);
			}
			writer.endRDF();
			
			if(!gid.isEmpty()) {
				
				sparql += "GRAPH <" + gid + "> {\n";
			}
			
			sparql += sw.toString();
			
			if(!gid.isEmpty()) {
				
				sparql += "}\n";
			}
		}
		
		sparql += "}";
		
		PostMethod method = new PostMethod(currentSession != null ? currentSession.getSessionURL() : getRepositoryURL());
		
		InputStream stream = null;
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			method.addRequestHeader("Accept", "application/json");
			method.setRequestBody(new NameValuePair[]{ new NameValuePair("query", sparql) });
			
			execute(method);
			
			// Integer.parseInt(method.getResponseBodyAsString(2048));
			
		} finally {
			
			IOUtils.closeQuietly(stream);
			method.releaseConnection();
			
		}
		
	}

	
	//all statements are treated as triples
	public int addStatements(AGraphSession currentSession, URI context,
			List<Statement> statements) throws Exception {

		PostMethod method = new PostMethod((currentSession != null ? currentSession.getSessionURL() : getRepositoryURL()) + "/statements?context=" + es("<" + context.stringValue() + ">"));
		
		try {
			method.addRequestHeader("Accept", "*/*");
			// method.addRequestHeader("Content-Type", "text/plain");
			
			method.setRequestEntity(new TriplesStatementsRequestEntity(statements));
			
			execute(method);
			
			return Integer.parseInt(method.getResponseBodyAsString(2048));
			
		} finally {
			method.releaseConnection();
		}
	}

	public static class TriplesStatementsRequestEntity implements RequestEntity {
		
		List<Statement> statements;

		public TriplesStatementsRequestEntity(List<Statement> statements) {
			super();
			this.statements = statements;
		}

		@Override
		public long getContentLength() {
			return -1;
		}

		@Override
		public String getContentType() {
			return "text/plain";
		}

		@Override
		public boolean isRepeatable() {
			return true;
		}

		@Override
		public void writeRequest(OutputStream outputStream) throws IOException {

			NTriplesWriter writer = new NTriplesWriter(outputStream);
			
			try {
				
				writer.startRDF();
				
				for(Statement stmt : statements) {
					
					writer.handleStatement(stmt);
					
				}
				
				writer.endRDF();
				
			} catch (RDFHandlerException e) {
				throw new IOException(e);
			}
			
		}
		
		
	}
	
	public static class QuadsStatementsRequestEntity implements RequestEntity {
		
		List<Statement> statements;
		
		public QuadsStatementsRequestEntity(List<Statement> statements) {
			super();
			this.statements = statements;
		}
		
		@Override
		public long getContentLength() {
			return -1;
		}
		
		@Override
		public String getContentType() {
			return "text/x-nquads";
		}
		
		@Override
		public boolean isRepeatable() {
			return true;
		}
		
		@Override
		public void writeRequest(OutputStream outputStream) throws IOException {
			
			NQuadsWriter writer = new NQuadsWriter(outputStream);
			
			try {
				
				writer.startRDF();
				
				for(Statement stmt : statements) {
					
					writer.handleStatement(stmt);
					
				}
				
				writer.endRDF();
				
			} catch (RDFHandlerException e) {
				throw new IOException(e);
			}
			
		}
		
		
	}

	public int remove(URI subjectURI, URI predicateURI, Value object,
			URI context) throws Exception {

		Map<String, String> params = new LinkedHashMap<String, String>();
		if(subjectURI != null) {
			params.put("subj", "<" + subjectURI.stringValue() + ">");
		}
		if(predicateURI != null) {
			params.put("pred", "<" + predicateURI.stringValue() + ">");
		}
		if(object != null) {
			// params.put("obj", object.stringValue());
			throw new RuntimeException("Object not supported!");
		}
		if(context != null) {
			params.put("context", "<" + context.stringValue() + ">");
		}
		
		DeleteMethod method = new DeleteMethod(getRepositoryURL() + "/statements?" + toFormString(params));
		
		try {
		
			method.addRequestHeader("Accept", "application/json");
			
			execute(method);
			
			return Integer.parseInt(method.getResponseBodyAsString(2048));
			
		} finally {
			
			method.releaseConnection();
			
		}
		
	}

	public void sparqlGraphNTripleOutput(String sparql, OutputStream os) throws Exception {

		PostMethod method = new PostMethod(getRepositoryURL());
		
		InputStream stream = null;
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			method.addRequestHeader("Accept", "text/plain");
			method.setRequestBody(new NameValuePair[]{ new NameValuePair("query", sparql) });
			
			execute(method);
			
			stream = method.getResponseBodyAsStream();
			
			IOUtils.copy(stream, os);
			
		} finally {
			
			IOUtils.closeQuietly(stream);
			method.releaseConnection();
			
		}
		
	}

	public void sparqlUpdate(AGraphSession currentSession, String sparql) throws Exception {


		PostMethod method = new PostMethod(currentSession != null ? currentSession.getSessionURL() : getRepositoryURL());
		
		InputStream stream = null;
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			method.addRequestHeader("Accept", "application/json");
			method.setRequestBody(new NameValuePair[]{ new NameValuePair("query", sparql) });
			
			execute(method);
			
		} finally {
			
			IOUtils.closeQuietly(stream);
			method.releaseConnection();
			
		}
		
	}

	public String createSession() throws Exception {

		PostMethod method = new PostMethod(getRepositoryURL() + "/session");
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			method.setRequestBody(new NameValuePair[]{ 
					new NameValuePair("autoCommit", "false"),
					new NameValuePair("lifetime", "300")
			});
			
			execute(method);
			
			return method.getResponseBodyAsString(4096);
			
		} finally {
			
			method.releaseConnection();
			
		}
		
	}

	public void rollbackTransaction(AGraphSession session) throws Exception {

		PostMethod method = new PostMethod(session.getSessionURL() + "/rollback");
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			
			execute(method);
			
		} finally {
			
			method.releaseConnection();
			
		}
		
	}

	public void commitTransaction(AGraphSession session) throws Exception {

		PostMethod method = new PostMethod(session.getSessionURL() + "/commit");
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			
			execute(method);
			
		} finally {
			
			method.releaseConnection();
			
		}
		
	}

	public void export(String segmentURI, BufferedOutputStream os) throws Exception {

		GetMethod method = new GetMethod( getRepositoryURL() + "/statements/query?context=" + es("<" + segmentURI + ">") );
		
		InputStream input = null;
		
		try {
			
			method.addRequestHeader("Accept", "text/plain");
			
			execute(method);
			
			input = method.getResponseBodyAsStream();
			
			IOUtils.copy(input, os);
			
		} finally {
			
			IOUtils.closeQuietly(input);
			
			method.releaseConnection();
			
		}
		
		
	}

	public int exportCompactString(String segmentURI, OutputStream outputStream) throws Exception {
		
		GetMethod method = new GetMethod( getRepositoryURL() + "/statements/query?context=" + es("<" + segmentURI + ">") );
		
		InputStream input = null;
		
		int c = 0;
		
		try {

			Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
			BlockCompactStringSerializer serializer = new BlockCompactStringSerializer(writer);
			
			method.addRequestHeader("Accept", "text/plain");
			
			execute(method);
			
			input = method.getResponseBodyAsStream();
			
			for( VitalNTripleIterator iterator = new VitalNTripleIterator(input); iterator.hasNext(); ) {
				
				GraphObject g  = iterator.next();
				
				serializer.startBlock();
				serializer.writeGraphObject(g);
				serializer.endBlock();
				c++;
				
			}
			
			writer.flush();
			
			return c;
			
		} finally {
			
			IOUtils.closeQuietly(input);
			
			method.releaseConnection();
			
		}
	}

	public int bulkImportBlockCompactStream(String graphURI,
			final InputStream inputStream) throws Exception {
		return bulkImportBlockCompactStream(graphURI, inputStream, "");
	}
	
	public int bulkImportBlockCompactStream(String graphURI,
			final InputStream inputStream, final String datasetURI) throws Exception {
		PostMethod method = new PostMethod(getRepositoryURL() + "/statements?context=" + es("<" + graphURI + ">"));
		
		final AtomicInteger iterated = new AtomicInteger(0);
		
		try {
			
			RequestEntity reqEntity = new RequestEntity(){

				@Override
				public long getContentLength() {
					return -1;
				}

				@Override
				public String getContentType() {
					return "text/plain";
				}

				@Override
				public boolean isRepeatable() {
					return false;
				}

				@Override
				public void writeRequest(OutputStream arg0) throws IOException {

					BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
					
					BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(arg0, StandardCharsets.UTF_8));
					
					//flush it more often ?
					
					int bytesWritten = 0;

					//100MB flush ?
					int flushEvery = 100 * 1024 * 1024;
					
					for( BlockIterator iterator = BlockCompactStringSerializer.getBlocksIterator(reader, false); iterator.hasNext(); ) {
						
						VitalBlock block = iterator.next();
						
						GraphObject mainObject = block.getMainObject();
						
						if( ! "".equals(datasetURI) ) {
						    mainObject.set(Property_hasProvenance.class, datasetURI);
						}
						
						String ntriplesString = mainObject.toRDF(RDFFormat.N_TRIPLE);
												
						writer.write( ntriplesString );
						
						bytesWritten +=
							ntriplesString.getBytes(StandardCharsets.UTF_8).length;

						iterated.incrementAndGet();
						
						for(GraphObject g : block.getDependentObjects()) {
							
							if( ! "".equals(datasetURI) ) {
							    g.set(Property_hasProvenance.class, datasetURI);
							}
							
							String rdf = g.toRDF(RDFFormat.N_TRIPLE);
							bytesWritten += rdf.getBytes(StandardCharsets.UTF_8).length;
							writer.write( rdf );
							
							iterated.incrementAndGet();
							
						}
						
						if(bytesWritten >= flushEvery ) {
							writer.flush();
							bytesWritten = 0;
						}
						
					}
					
					writer.flush();
					
					
				}};
				
			method.addRequestHeader("Content-Type", "text/plain");
			method.setRequestEntity(reqEntity);
				
			execute(method);
			
			return iterated.get();
			
		} finally {
			
			method.releaseConnection();
			
		}
		
	}
	
	/*

	public void pingSession(AGraphSession currentSession) throws Exception {

		PostMethod method = new PostMethod(getRepositoryURL() + "/session/ping");
		
		try {
			
			method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
			
			execute(method);
			
			String r = method.getResponseBodyAsString(2048);
			
			System.out.println(r);
			
		} finally {
			
			method.releaseConnection();
			
		}
		
	}
	*/
	
}

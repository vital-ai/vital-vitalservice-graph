package ai.vital.triplestore.allegrograph;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryResultHandler;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONParser;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.ntriples.NTriplesUtil;
import org.openrdf.rio.ntriples.NTriplesWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.code.externalsorting.ExternalSort;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import ai.vital.allegrograph.client.AGraphClient;
import ai.vital.allegrograph.client.AGraphClientConfig;
import ai.vital.allegrograph.client.AGraphSession;
import ai.vital.allegrograph.client.GraphObjectsRequestEntity;
import ai.vital.triplestore.allegrograph.query.ExportQueryImplementation;
import ai.vital.triplestore.allegrograph.query.GraphQueryImplementation;
import ai.vital.triplestore.allegrograph.query.SparqlQueryImplementation;
import ai.vital.triplestore.model.VitalServiceTypes;
import ai.vital.triplestore.sparql.SparqlBatch;
import ai.vital.vitalservice.VitalServiceConstants;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.exception.VitalServiceException;
import ai.vital.vitalservice.query.CollectStats;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.QueryTime;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExportQuery;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.RDFStatement;
import ai.vital.vitalsigns.model.VITAL_Container;
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject;
import ai.vital.vitalsigns.model.VitalSegment;
import ai.vital.vitalsigns.model.VitalTransaction;
import ai.vital.vitalsigns.model.properties.Property_hasProvenance;
import ai.vital.vitalsigns.model.properties.Property_hasSegmentID;
import ai.vital.vitalsigns.model.properties.Property_hasTransactionID;
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.model.property.StringProperty;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.rdf.VitalNTripleIterator;

/**
 * Allegrograph wrapper stores the segments list in a separate segment
 *
 */

public class AllegrographWrapper {

	public final static String ROOT_CATALOG_NAME = "";
	
	private final static Logger log = LoggerFactory.getLogger(AllegrographWrapper.class);
	
	public final static String SEGMENTS_LIST_GRAPH_URI = "vitalservice:segments";
	
	public final static ValueFactory vFactory = new ValueFactoryImpl();

	public final static URI systemRepositoryContext = vFactory.createURI(SEGMENTS_LIST_GRAPH_URI);
	
	AGraphClient agClient = null;
	
	AGraphClientConfig config = null;
	
	// protected AGraphSession currentSession;
	
	protected Map<String, AGraphSession> txMap = Collections.synchronizedMap(new HashMap<String, AGraphSession>());
	
	private AllegrographWrapper(String _serverURL, String _username, String _password, String catalogName, String repositoryName, Integer maxConnectionsCount)  {
		
		if(_serverURL == null) throw new NullPointerException("serverURL cannot be null");
		if(_username == null) throw new NullPointerException("username cannot be null");
		if(_password == null) throw new NullPointerException("password cannot be null");
		// if(catalogName == null) throw new NullPointerException("Catalog name cannot be null");
		if(repositoryName == null) throw new NullPointerException("Repository name cannot be null");
		config = new AGraphClientConfig(_serverURL, catalogName, repositoryName, _username, _password, maxConnectionsCount);
		
	}
	
	public static AllegrographWrapper create(String _serverURL, String _username, String _password, String catalogName, String repositoryName, Integer maxConnectionsCount) {
		return new AllegrographWrapper(_serverURL, _username, _password, catalogName, repositoryName, maxConnectionsCount);
	}
	
	private static void securityCheck() throws SecurityException {
		Set<String> classes = new HashSet<String>(Arrays.asList(
			"ai.vital.vitalservice.factory.VitalServiceFactory",
			"ai.vital.vitalservice.superadmin.factory.VitalServiceSuperAdminFactory"
		));
		
		StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
		
		if(stackTraceElements == null) stackTraceElements = new StackTraceElement[0];
		
		boolean passed = false;
		
		for(StackTraceElement el : stackTraceElements) {
			
			if(classes.contains(el.getClassName())) {
				passed = true;
				break;
			}
		}
		
		if(!passed) throw new SecurityException("Cannot instantiate allegrograph wrapper outside of classes: " + classes);
	}
	
	/**
	 * Purges the repository
	 */
	
	public void purge() throws Exception {

		_checkOpen();
				
		log.info("Purging repository: " + config.getRepositoryName() + "...");
		
		agClient.remove(null, null, null, null);
		
		log.info("Purge complete");

	}
	
	public synchronized void open() throws Exception {

		log.info("Opening AGraph wrapper...");
		
		agClient = new AGraphClient(config);
		
		// prepare repository
		agClient.createRepository(false);
		
	}
	
	public synchronized void close() throws Exception {
		
		_checkOpen();
		
		log.info("Closing AG wrapper ...");
		
		this.agClient = null;
		
	}
	
	private void _checkOpen() throws Exception {
		if(this.agClient == null) throw new Exception("AG wrapper not open!");
	}

	@SuppressWarnings("deprecation")
	public GraphObject save(VitalTransaction transaction, VitalSegment targetSegment, GraphObject graphObject, List<VitalSegment> segmentsPool) throws Exception {

		_checkOpen();
		
		AGraphSession currentSession = txSession(transaction);
		
		targetSegment = checkSegment(targetSegment);
		
		if(graphObject instanceof RDFStatement) {
			throw new Exception("cannot save rdf statement as graph object in allegrograph");
		}
		
		String segmentURI = targetSegment.getURI();
		
		String triplesQuery = getDirectTypeTriplesQueryString(Arrays.asList(graphObject.getURI()), toSegmentsURIs(segmentsPool));

		final List<BindingSet> results = new ArrayList<BindingSet>();
		
		SPARQLResultsJSONParser parser = new SPARQLResultsJSONParser();
		
		parser.setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
			
			@Override
			public void handleSolution(BindingSet bs)
					throws TupleQueryResultHandlerException {
				results.add(bs);
			}
			
		});
		
		agClient.sparqlSelectJsonOutput(triplesQuery, parser);
		
		
		boolean resultExistsInSegment = false;
					
		String otherSegment = null;
					
		for( BindingSet bs : results) {
							
			String gURI = bs.getValue("g").stringValue();
							
			if( segmentURI.equals(gURI) ) {
				resultExistsInSegment = true;
			} else {
				otherSegment = gURI;
			}
		}
				
		if(otherSegment != null) {
			throw new Exception("Object with URI: " + graphObject.getURI() + " already exists in other segment: " + getSegmentID(segmentsPool, otherSegment));
		}
			
		if(resultExistsInSegment) {
			
			Set<String> _set = new HashSet<String>();
			
			_set.add(graphObject.getURI());
			
			deleteBatch(transaction, new String[]{segmentURI}, _set);
			
			
		}
		
		agClient.addTriples(currentSession, segmentURI, new GraphObjectsRequestEntity(Arrays.asList(graphObject)));
		
		return graphObject;
		
	}
			
	private String getSegmentID(List<VitalSegment> segmentsPool,
			String segmentURI) {

		for(VitalSegment s : segmentsPool) {
			if(s.getURI().equals(segmentURI)) {
				return (String) s.getRaw(Property_hasSegmentID.class);
			}
		}
		
		return null;
	}

	private VitalSegment checkSegment(VitalSegment targetSegment) throws Exception {

		List<String> listAllSegments = listAllSegments();
		
		if(listAllSegments.contains(targetSegment.getURI())) return targetSegment;
		
		throw new RuntimeException("Segment with URI: " + targetSegment.getURI() + " not found");
		
	}

	public VitalStatus delete(VitalTransaction tx, List<VitalSegment> segmentsPool, List<URIProperty> uris)
			throws Exception {
				
		_checkOpen();
		
		Set<String> urisSet = new HashSet<String>();
			
		for(URIProperty vu : uris) {
			urisSet.add(vu.get());
		}
			
		//merged operations over all user segments
		deleteBatch(tx, toSegmentsURIs(segmentsPool), urisSet);
			
		return VitalStatus.withOK();
		
	}
			
	protected String[] toSegmentsURIs(List<VitalSegment> segments) {
		
		List<String> uris = new ArrayList<String>();
		
		for(VitalSegment s : segments) {
			uris.add(s.getURI());
		}
		
		return uris.toArray(new String[uris.size()]);
		
	}
		
	public VitalStatus ping() throws Exception {
		
		_checkOpen();
		
		long start = System.currentTimeMillis();
		VitalStatus status = VitalStatus.withOKMessage("Version: " + agClient.getVersion());
		status.setPingTimeMillis((int) (System.currentTimeMillis() - start));
		return status;
		
	}
		
	List<VITAL_Container> _emptyList = new ArrayList<VITAL_Container>();
	
	public GraphObject get(List<VitalSegment> segmentsPool, URIProperty uri) throws Exception {

		List<GraphObject> objs = _getBatch(toSegmentsURIs(segmentsPool), Arrays.asList(uri.get()), null);

		if(objs.size() > 0) return objs.get(0);
		
		return null;
		
	}
			
	/*
	private void ontologyIRISetter(GraphObject graphObject, Model model) throws Exception {
		
		// if(true) return
		DomainOntology _do = VitalSigns.get().getClassDomainOntology(graphObject.getClass());
		if(_do == null) throw new Exception("Domain ontology for class: " + graphObject.getClass().getCanonicalName() + " not found");
		graphObject.setProperty("ontologyIRI", _do.getUri());
		graphObject.setProperty("versionIRI", _do.toVersionString());
		graphObject.toRDF(model);
		graphObject.setProperty("ontologyIRI", null);
		graphObject.setProperty("versionIRI", null);

		
	}
	*/
		
	/*
	private void ontologyIRIVersionsCheck(GraphObject graphObject) {
		
		// if(true) return
		
		String ontologyIRI = graphObject.getProperty("ontologyIRI") != null ? graphObject.getProperty("ontologyIRI").toString() : null;
		String versionIRI = graphObject.getProperty("versionIRI") != null ? graphObject.getProperty("versionIRI").toString() : null;
		
		if(ontologyIRI != null && versionIRI != null) {
			
			DomainOntology _cdo = VitalSigns.get().getDomainOntology(ontologyIRI);
			
			if(_cdo != null) {
				
				DomainOntology _do = new DomainOntology(ontologyIRI, versionIRI);
				
				int comp = _do.compareTo(_cdo);
				
				if( comp > 0 ) {
					
					log.error("Domain ontology " + ontologyIRI + " object version newer than loaded: " + _do.toVersionIRI() + " > " + _cdo.toVersionIRI() + ", object class: " + graphObject.getClass().getCanonicalName() + ", uri: " + graphObject.getURI());
					
				}
				
			} else {
			
				log.error("Domain ontology with IRI not found: " + ontologyIRI + ", object class: " + graphObject.getClass().getCanonicalName() + ", uri: " + graphObject.getURI());
				
			}
			
		}
		
		graphObject.setProperty("ontologyIRI", null);
		graphObject.setProperty("versionIRI", null);
		
	}
	*/
			
	protected String getDirectTypeTriplesQueryString(Collection<String> uris, String[] context) {
					
		String graphsString = "";
					
		// agraph 4.14.1 required from NAMED
		if(context != null) {
			for(String u : context) {
				graphsString += (" FROM NAMED <" + u + "> \n");
			}
		}
		
		String p = "<" + RDF.TYPE.stringValue() + ">";
		
		StringBuilder l = new StringBuilder();
		for(String uri : uris) {
			if( l.length() > 0 ) l.append(", ");
			l.append('<').append(uri).append('>');
		}
		
		return "SELECT ?u ?g ?o \n" + graphsString + " WHERE { GRAPH ?g { ?u " + p + " ?o . FILTER( ?u IN ( " + l.toString() + " ) ) } }";
		
	}
	
	
	public ResultList save(VitalTransaction tx, VitalSegment targetSegment, List<GraphObject> graphObjectsList, List<VitalSegment> segmentsPool) throws Exception {

		if(targetSegment == null) throw new NullPointerException("target segment cannot be null");
		
		
		if(graphObjectsList.size() == 0) {
			ResultList rl = new ResultList();
			rl.setStatus(VitalStatus.withOKMessage("Empty input list"));
			return rl;
		}
			
		_checkOpen();
		
		AGraphSession currentSession = txSession(tx);
				
		// analyze results
		for(GraphObject g : graphObjectsList) {
			if(g instanceof RDFStatement) {
				throw new Exception("cannot save rdf statement as graph object in allegrograph");
			}
		}
		
		
		List<String> uris = new ArrayList<String>();
		
		for(int i = 0 ; i < graphObjectsList.size(); i++) {
			GraphObject g = graphObjectsList.get(i);
			if(g == null) throw new NullPointerException("one of graph object is null, index: " + i);
			if(g.getURI().isEmpty()) throw new NullPointerException("one of graph objects's URI is null or empty, index: " + i);
			if(uris.contains(g.getURI())) throw new NullPointerException("more than 1 graph object with same uri in input collection: " + g.getURI());
			uris.add(g.getURI());
		}
		
		ResultList l = new ResultList();
		
		//check if object exists in different segment
			
		String[] segmentURIs = toSegmentsURIs(segmentsPool);
			
		String segmentURI = targetSegment.getURI();
			
		//select all <graph> <uri> <rdf:type> tuples
			
		Set<String> toDeleteFromSegment = new HashSet<String>();

		String directTypeTriplesQueryString = getDirectTypeTriplesQueryString(uris, segmentURIs);
		
		final List<BindingSet> results = new ArrayList<BindingSet>();
		
		SPARQLResultsJSONParser parser = new SPARQLResultsJSONParser();
		parser.setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
			
			@Override
			public void handleSolution(BindingSet bs)
					throws TupleQueryResultHandlerException {
				results.add(bs);
			}
			
		});
		
		
		agClient.sparqlSelectJsonOutput(directTypeTriplesQueryString, parser);

		for(BindingSet bs : results) {
			
			String segURI = bs.getValue("g").stringValue();
			
			String uri = bs.getValue("u").stringValue(); 
					
			if(segmentURI.equals(segURI)) {
				toDeleteFromSegment.add(uri);
			} else {
				throw new Exception("Object with URI: " + uri + " already exists in another segment: " + getSegmentID(segmentsPool, segURI));
			}
						
		}
			
		if(toDeleteFromSegment.size() > 0) {
			deleteBatch(tx, new String[] { segmentURI }, toDeleteFromSegment);
		}			

		agClient.addTriples(currentSession, segmentURI, new GraphObjectsRequestEntity(graphObjectsList));
		
		for(GraphObject g : graphObjectsList) {
			l.getResults().add(new ResultElement(g, 1D));
		}
		
		return l;
	}
		
	private AGraphSession txSession(VitalTransaction tx) throws Exception {

		if(tx == null || tx == VitalServiceConstants.NO_TRANSACTION || tx.getURI().equals(VitalServiceConstants.NO_TRANSACTION.getURI())) return null;
		
		String id = (String) tx.getRaw(Property_hasTransactionID.class);
		if(id == null) throw new Exception("No transactionID in a transaction object");
		
		AGraphSession session = txMap.get(id);
		
		if(session == null) throw new Exception("Transaction not found: " + id);
		
		return session;
		
	}

	public VitalStatus delete(VitalTransaction tx, List<VitalSegment> segmentsPool, URIProperty uri)
			throws Exception {
		
		if(uri.get().startsWith(URIProperty.MATCH_ALL_PREFIX)) {
			
			throw new VitalServiceException("DELETE ALL should be handled at the upper service level");
			
			/*
			String organizationID = (String) organization.getRaw(Property_hasOrganizationID.class);
			String appID = (String) app.getRaw(Property_hasAppID.class);
			
			_checkOpen();
			
			String segmentID = uri.get().substring(URIProperty.MATCH_ALL_PREFIX.length());
			if(segmentID.length() < 1) throw new Exception("No segment provided in special match-all case!");
			
			VitalSegment seg = VitalSegment.withId(segmentID);
			
			String sid = toSegmentURI(organization, app, seg);
			
			VitalSegment s = _getSegment(organizationID, appID, segmentID);
						
			if ( s == null ) throw new Exception("Segment " + segmentID + " in app " + appID + " of organization " + organizationID + " not found");
			
			int deleted = agClient.deleteGraph(currentSession, sid);
			
			VitalStatus status = VitalStatus.withOKMessage("All segment " + segmentID + " objects deleted: " + deleted);
			status.setSuccesses(deleted);
			return status;
			*/
		}
				
		return delete(tx, segmentsPool, Arrays.asList(uri));
	}
		
	public VitalStatus deleteAll(VitalSegment targetSegment) throws Exception {
		
		String segmentID = (String) targetSegment.getRaw(Property_hasSegmentID.class);
				
		int deleted = agClient.deleteGraph(null, targetSegment.getURI());
		
		VitalStatus status = VitalStatus.withOKMessage("All segment URI " + targetSegment.getURI() + " id: " + segmentID + " objects deleted: " + deleted);
		status.setSuccesses(deleted);
		return status;
		
	}
			
	public List<GraphObject> getBatch(List<VitalSegment> segmentsPool, Collection<String> uris) throws Exception {

		if(uris.size() < 1) return Collections.emptyList();
		
		_checkOpen();
		
		String[] segmentsURIs = toSegmentsURIs(segmentsPool);
			
		return _getBatch(segmentsURIs, uris, null);
			
	}
	
	public List<GraphObject> _getBatch(String[] segmentsURIs, Collection<String> uris, QueryStats queryStats) throws Exception {
		
		final List<GraphObject> objects = new ArrayList<GraphObject>();
		
		if(uris.size() < 1) return objects;
		
		
		int batch = 1000;		
		
		List<String> allURIs = (List<String>) (uris instanceof List ? uris : new ArrayList<String>(uris));
		
		int p = 0;
		
		for(int i = 0 ; i < allURIs.size(); i += batch) {
			
			long start = System.currentTimeMillis();
			
			p++;
			
			List<String> sublist = allURIs.subList(i, Math.min( i + batch, allURIs.size()));
			
			String hugeSparql = SparqlBatch.getResourceSingleBatchSELECTSparql(segmentsURIs, sublist);

			
			long opStart = System.currentTimeMillis();
			
			SPARQLResultsJSONParser parser = new SPARQLResultsJSONParser(vFactory);
			
			parser.setQueryResultHandler(new QueryResultHandler() {
			
				NTriplesWriterFactory nTriplesWriterFactory = new NTriplesWriterFactory();
				
				String lastSubject = null;
				
				StringWriter sw = null;
				RDFWriter writer = null;
				
				
				@Override
				public void startQueryResult(List<String> arg0) throws TupleQueryResultHandlerException {}
				
				@Override
				public void handleSolution(BindingSet bs) throws TupleQueryResultHandlerException {
					
					URI subjectURI = (URI) bs.getValue("s");
					URI predicateURI = (URI) bs.getValue("p");
					Value value = bs.getValue("o");
					
					String subjectURIString = subjectURI.stringValue();
					
					if(lastSubject != null && !subjectURIString.equals(lastSubject)) {
						
						//flush previous object
						try {
							flushObject();
						} catch (RDFHandlerException e) {
							throw new RuntimeException(e);
						}
						
					}
					
					if(writer == null) {
						sw = new StringWriter();
						writer = nTriplesWriterFactory.getWriter(sw);
						try {
							writer.startRDF();
						} catch (RDFHandlerException e) {
							throw new RuntimeException("");
						}
					}

					try {
						writer.handleStatement( new StatementImpl(subjectURI, predicateURI, value) );
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
					
					lastSubject = subjectURIString;
					
					
				}
				
				void flushObject() throws RDFHandlerException {
					
					if(sw == null) return;
					
					writer.endRDF();
					
					Model m = ModelFactory.createDefaultModel();
					m.read(new StringReader(sw.toString()), null, "N-TRIPLE");
					GraphObject g = VitalSigns.get().readGraphObject(lastSubject, m);
						
					if(g != null) {
						// ontologyIRIVersionsCheck(g);
						objects.add(g);
					}
					m.close();
					writer = null;
					sw = null;
					
					// m.removeAll()
					
				}
				
				@Override
				public void handleLinks(List<String> arg0) throws QueryResultHandlerException {}
				
				@Override
				public void handleBoolean(boolean arg0) throws QueryResultHandlerException {}
				
				@Override
				public void endQueryResult() throws TupleQueryResultHandlerException {
					try {
						flushObject();
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}
			});
			
			agClient.sparqlSelectJsonOutput(hugeSparql, parser);

			if(queryStats != null) {
				long time = queryStats.addObjectsBatchGetTimeFrom(opStart);
				if(queryStats.getQueriesTimes() != null) queryStats.getQueriesTimes().add(new QueryTime("Batch get objects page " + p, hugeSparql, time));
			}
			
			/*
			LinkedHashMap<String, Object> namesValues = agClient.sparqlSelectSimpleJsonOutput(hugeSparql);

			if(queryStats != null) {
				long time = queryStats.addObjectsBatchGetTimeFrom(opStart);
				if(queryStats.getQueriesTimes() != null) queryStats.getQueriesTimes().add(new QueryTime("Batch get objects page " + p, hugeSparql, time));
			}
			
			StringWriter sw = null;
				
			// Model m = null
				
			String lastSubject = null;
			List<List> vals = (List)namesValues.get("values");
			for(List val : vals) {
					
				if(lastSubject != null && !lastSubject.equals(val.get(0))) {
					Model m = ModelFactory.createDefaultModel();
					m.read(new StringReader(sw.toString()), null, "N-TRIPLE");
					GraphObject g = VitalSigns.get().readGraphObject(lastSubject.substring(1, lastSubject.length()-1), m);
						
					if(g != null) {
						// ontologyIRIVersionsCheck(g);
						objects.add(g);
					}
						
					sw = null;
					// m.removeAll()
						
				}
					
				if(sw == null) sw = new StringWriter();
					
				// if(m == null) m = ModelFactory.createDefaultModel();
					
				// m.read(new StringReader(val[0] + " " + val[1] + " " + val[2] + " .\n"), null, "N-TRIPLE")
					
				sw.append(val.get(0) + " " + val.get(1) + " " + val.get(2) + " .\n");
					
				lastSubject = (String) val.get(0);
					
			}
				
			// if(m != null && m.size() > 0) {
			if(sw != null) {
				Model m = ModelFactory.createDefaultModel();
				m.read(new StringReader(sw.toString()), null, "N-TRIPLE");
				GraphObject g = VitalSigns.get().readGraphObject(lastSubject.substring(1, lastSubject.length()-1), m);
					
				if(g != null) {
					// ontologyIRIVersionsCheck(g);
					objects.add(g);
				}
			}

			*/
			
			log.debug("Resolved batch URIs: {} - {} of {}, {} ms", new Object[] {i, i+batch, uris.size(), System.currentTimeMillis() - start} );
		}
		
		return objects;
		
	}
			
	public List<String> listAllSegments() throws Exception {
				
		_checkOpen();
		
		final List<String> r = new ArrayList<String>();
		
		//TODO
		//select all segment objects
			String queryString = "" +
"SELECT DISTINCT ?s \n" +
"FROM <" + SEGMENTS_LIST_GRAPH_URI + "> \n" +
"WHERE { \n" +
"	?s <" + RDF.TYPE.stringValue() + "> <" + VitalServiceTypes.Segment + "> . \n" + 
"}";
			
		SPARQLResultsJSONParser parser = new SPARQLResultsJSONParser(vFactory);
		parser.setQueryResultHandler(new QueryResultHandler() {
			
			@Override
			public void startQueryResult(List<String> arg0)
					throws TupleQueryResultHandlerException {}
			
			@Override
			public void handleSolution(BindingSet arg0)
					throws TupleQueryResultHandlerException {
				r.add(arg0.getValue("s").stringValue());
			}
			
			@Override
			public void handleLinks(List<String> arg0)
					throws QueryResultHandlerException {}
			
			@Override
			public void handleBoolean(boolean arg0) throws QueryResultHandlerException {}
			
			@Override
			public void endQueryResult() throws TupleQueryResultHandlerException {}
		});
		agClient.sparqlSelectJsonOutput(queryString, parser);
			
		return r;
			
	}
		
	public VitalSegment addSegment(VitalSegment segment) throws Exception {

		if( listAllSegments().contains(segment.getURI())) throw new Exception("Segment already exists: " + segment.getURI());
		
		List<Statement> statements = new ArrayList<Statement>();
			
			
		// model.add(ResourceFactory.createResource(toSegmentURI(dbSegment)), com.hp.hpl.jena.vocabulary.RDF.type, SegmentURI)
			
		statements.add(new StatementImpl(vFactory.createURI(segment.getURI()), RDF.TYPE, vFactory.createURI(VitalServiceTypes.Segment)));
		
		agClient.addStatements(null, systemRepositoryContext, statements);
		
		return segment;
		
	}
		
	public VitalStatus removeSegment(VitalSegment segment, boolean deleteData) throws Exception {
		
		if( ! listAllSegments().contains(segment.getURI())) throw new Exception("Segment not found: " + segment.getURI());
		
		agClient.remove(vFactory.createURI(segment.getURI()), null, null, systemRepositoryContext);
		
		if(deleteData) {
			agClient.deleteGraph(null, segment.getURI());
		}
			
		return VitalStatus.withOK();
	}
		
	static boolean SCAN_TEST_MODE = false;
	
	public void scanSegment(VitalSegment segment, int limit, ScanListener handler) throws Exception {

		segment = checkSegment(segment);
		
		File dumpFile = null;
		
		File sortedFile = null;
		BufferedOutputStream os = null;
		
		VitalNTripleIterator iterator = null;
		
		try {

			dumpFile = File.createTempFile("agscan", ".nt");
			dumpFile.deleteOnExit();
			log.info("Dumping segment to temp file: " + dumpFile.getAbsolutePath() + " ...");
			os = new BufferedOutputStream(new FileOutputStream(dumpFile));
			
			String segmentURI = segment.getURI(); 
			
			long start = System.currentTimeMillis();
			
			// String sparql = "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <" + segmentURI + "> { ?s ?p ?o } } ORDER BY ASC(?s)";
			// agClient.sparqlGraphNTripleOutput(sparql, os);
			
			agClient.export(segmentURI, os);
			// conn.export(rdfHandler, conn.getValueFactory().createURI(segmentURI));
			
			// agClient.sparqlGraphQuery
			
			os.close();
			
			log.info("Dump complete, time: {}ms", System.currentTimeMillis() - start);
			log.info("Sorting output n-triples...");
			
			start = System.currentTimeMillis();
			
			sortedFile = File.createTempFile("agscansort", ".nt");
			
			ExternalSort.sort(dumpFile, sortedFile);
			
			FileUtils.deleteQuietly(dumpFile);
			dumpFile = null;
			
			log.info("Sort complete, time: {}ms", System.currentTimeMillis() - start);
			
			log.info("Iterating over ntriples blocks...");
			
			List<GraphObject> batchRes = new ArrayList<GraphObject>(limit+1);

			// sortedFile = dumpFile;
			
			for( iterator = new VitalNTripleIterator(sortedFile); iterator.hasNext(); ) {
				
				GraphObject g = iterator.next();
				
				if(g == null) continue;
				
				batchRes.add(g);
				
				if(batchRes.size() == limit) {
					handler.onBatchReady(batchRes);
					batchRes = new ArrayList<GraphObject>(limit+1);
				}
				
			}
			
			if(batchRes.size() > 0) {
				handler.onBatchReady(batchRes);
			}
			
			handler.onScanComplete();
		
		} finally {

			IOUtils.closeQuietly(iterator);	
		
			FileUtils.deleteQuietly(dumpFile);
			FileUtils.deleteQuietly(sortedFile);
		
			IOUtils.closeQuietly(os);
		
		}
				
	}
	
	// will wotk as soon as AGraph export returns triples sorted by subject 
	/*
	public void scanSegmentBug(VitalOrganization organization, App app, VitalSegment segment,
			int limit, ScanListener handler) throws Exception {

		segment.setOrganizationID(organization.getID());
		segment.setAppID(app.getID());
		
		String segmentURI = toSegmentURI(segment);
		
		TupleQueryResult result = null;
		
		try {
		
			// int offset = 0
			
			// while(offset >= 0) {
		
				long stage = System.currentTimeMillis();
						
				// String sparql = "SELECT DISTINCT ?s WHERE { GRAPH <${segmentURI}> {?s ?p ?o} } LIMIT ${limit} OFFSET ${offset}"

				List<GraphObject> batchRes = [] 
				
				// BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("out.nt"))
				// NTriplesWriter writer = new NTriplesWriter(bos)
				
				RDFHandler rdfHandler = new RDFHandler(){
					
					private total = 0
					
					private String lastSubject = null
					private List<Statement> lastStataments = []
					
					private Model model = ModelFactory.createDefaultModel()
					
					public void endRDF(){
						if(batchRes.size() > 0) {
							handler.onBatchReady(batchRes)
						}
					// writer.endRDF()
					}
					public void handleComment(String comment){}
					public void handleNamespace(String ns, String prefix){}
					public void handleStatement(Statement statement) {
						
					// writer.handleStatement(statement)
						
						String subject = statement.subject.stringValue()
						if( lastSubject != null && lastSubject != subject ) {
							
							// convert to model
							// for(Statement lastStmt : lastStataments) {
								
							// }
							
							if(SCAN_TEST_MODE) {
									
								VITAL_Node g = new VITAL_Node()
								g.URI = lastSubject
								batchRes.add(g)
								
								if(batchRes.size() >= limit) {
									
									total += batchRes.size()
									
									println("Got another batch: ${batchRes.size()} - total: ${total} ${System.currentTimeMillis()-stage}ms")
									
									stage = System.currentTimeMillis()
									handler.onBatchReady(batchRes)
									batchRes = []
								}
								
							} else {

								for(Statement stmt : lastStataments) {
									
									model.add(Convert2.statementToTriple(stmt))									
								}
							
								GraphObject g = VitalSigns.get().readGraphObject(lastSubject, model)
								
								model.removeAll()
								
								if(g == null) {
									
									log.warn("Incomplete object: ${lastStataments}")
									
								} else {
									batchRes.add(g)
									
									if(batchRes.size() >= limit) {
										
										total += batchRes.size()
												
										stage = System.currentTimeMillis()
										handler.onBatchReady(batchRes)
										batchRes = []
										
									}
								}
							}
							
							lastStataments.clear()
						}
						
						lastSubject = subject
						
						lastStataments.add(statement)
					}
					public void startRDF(){
						// writer.startRDF()
					}
				};
			
				conn.export(rdfHandler, conn.getValueFactory().createURI(segmentURI));
			
				handler.onScanComplete()
			
		} finally {
		
			if(result != null) try { result.close() } catch(Exception e) {}
		
			conn.close()
		}
		
	}
	*/
			
	public static interface ScanListener {
				
		public void onBatchReady(List<GraphObject> part);
				
		public void onScanComplete();
				
	}

	public void deleteBatch(VitalTransaction tx, String[] context, Set<String> uris) throws Exception {

		if(uris.size() == 0) return;
		
		if(context == null) context = new String[]{null};
			
		int step = 25;
		
		AGraphSession currentSession = txSession(tx);
		
		for(String ctx : context) {

				//single named graph at a time
						
			List<String> _list = new ArrayList<String>(uris);
				
			for(int i = 0; i < _list.size(); i += step) {
				
				int lastIndex = Math.min(_list.size(), i + step);
					
				List<String> _subList = new ArrayList<String>(_list.subList(i, lastIndex));
					
				log.debug("Deleting uris " + (i+1) + " - " + lastIndex + " of " + uris.size() + "...");
					
					
				String[] subContext = ctx != null ? new String[]{ctx} : null;
					
				//no graph context here
				String deleteSparql = SparqlBatch.getDeleteBatchSparql(subContext, _subList);
				long start = System.currentTimeMillis();
				agClient.sparqlUpdate(currentSession, deleteSparql);
				long stop = System.currentTimeMillis();
					
				log.debug("Delete batch response " + (stop-start) + "ms");
					
			}
		}		
				
	}
	
	public ResultList sparqlQuery(VitalSparqlQuery query) throws Exception {
		
		if(query.isReturnSparqlString()) {
			ResultList rl = new ResultList();
			rl.setStatus(VitalStatus.withOKMessage(((VitalSparqlQuery)query).getSparql()));
			return rl;
		}
		
		return SparqlQueryImplementation.handleSparqlQuery(this, agClient, (VitalSparqlQuery)query);
		
	}
	
	private QueryStats initStatsObject(VitalQuery sq) {
		
		QueryStats queryStats = null;
		if(sq.getCollectStats() == CollectStats.normal || sq.getCollectStats() == CollectStats.detailed) {
			queryStats = new QueryStats();
		}
		if(sq.getCollectStats() == CollectStats.detailed) {
			queryStats.setQueriesTimes(new ArrayList<QueryTime>());
		} else {
			if(queryStats != null) {
				queryStats.setQueriesTimes(null);
			}
		}
		
		return queryStats;
	}

	
	public ResultList graphQuery(VitalGraphQuery query) throws Exception {

		QueryStats queryStats = initStatsObject(query);
		
		//main select query implementation
		if(!query.isReturnSparqlString()) {

			if(query.getSegments() == null || query.getSegments().size() < 1) throw new NullPointerException("query segments list cannot be null or empty");
					
			List<String> currentSegments = listAllSegments();
			
			for(VitalSegment x : query.getSegments()) {
					
				boolean found = false;
								
				String segmentID = (String) x.getRaw(Property_hasSegmentID.class);
				
				for(String s : currentSegments) {
					
					if(s.equals(x.getURI())) {
						found = true;
						break;
					}
									
				}
						
				if(!found) throw new Exception("Segment not found URI: " + x.getURI() + " id: " + segmentID);
						
			}
			
		}
				
		GraphQueryImplementation impl = new GraphQueryImplementation(this, agClient, query, queryStats);
					
		return impl.handle();
					
	}
	
	public ResultList selectQuery(VitalSelectQuery query) throws Exception {
		
		QueryStats queryStats = initStatsObject(query);
		
		if(!query.isReturnSparqlString()) {

			if(query.getSegments() == null || query.getSegments().size() < 1) throw new NullPointerException("query segments list cannot be null or empty");
								
			List<String> currentSegments = listAllSegments();
			
			for(VitalSegment x : query.getSegments()) {
					
				boolean found = false;
								
				String segmentID = (String) x.getRaw(Property_hasSegmentID.class);
				
				for(String s : currentSegments) {
					
					if(s.equals(x.getURI())) {
						found = true;
						break;
					}
									
				}
						
				if(!found) throw new Exception("Segment not found URI: " + x.getURI() + " id: " + segmentID);
						
			}
				
		}
			
		if(query instanceof VitalExportQuery) {
			return ExportQueryImplementation.handleExportQuery(this, agClient, (VitalExportQuery) query, queryStats);
		}
			
		GraphQueryImplementation impl = new GraphQueryImplementation(this, agClient, query, queryStats);
			
		return impl.handle();
			
	}
	
	
	@SuppressWarnings("deprecation")
	public int getSegmentSize(VitalSegment segment) throws Exception {

		segment = checkSegment(segment);
		
		URI segmentURI = vFactory.createURI(segment.getURI());
			
		String sparql = "select (COUNT(DISTINCT ?s) AS ?count) where { graph <" + segmentURI.toString() + "> { ?s ?p ?o } }";
		
		final AtomicInteger result = new AtomicInteger(-1);
				
		SPARQLResultsJSONParser parser = new SPARQLResultsJSONParser();
		parser.setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
			
			@Override
			public void handleSolution(BindingSet bs)
					throws TupleQueryResultHandlerException {
				
				Value v = bs.getValue("count");
				
				if(v instanceof Literal) {
					BigInteger integerValue = ((Literal)v).integerValue();
					result.set(integerValue.intValue());
				}
				
			}
			
		});
		agClient.sparqlSelectJsonOutput(sparql, parser);
		
		if(result.get() < 0) throw new RuntimeException("Sparql query didn't return count results");
		
		return result.get();
			
	}

	public ResultList insertRDFStatements(VitalTransaction transaction, List<GraphObject> rdfStatementList) throws Exception {

		AGraphSession currentSession = txSession(transaction);
		
		List<Statement> stmts = new ArrayList<Statement>();
		
		for(GraphObject g : rdfStatementList) {
			
			if(!( g instanceof RDFStatement)) {
				throw new Exception("cannot mix rdf statements with other graph objects in allegrograph");
			}
			
			RDFStatement stmt = (RDFStatement)g;
			
			if( stmt.getProperty("rdfSubject") == null || stmt.getProperty("rdfPredicate") == null || stmt.getProperty("rdfObject") == null) {
				throw new Exception("Incomplete RDF statement: " + stmt);
			}
			
			StatementImpl impl = null;
			
			if(stmt.getProperty("rdfContext") != null) {
				impl = new ContextStatementImpl(
					NTriplesUtil.parseResource(
						((IProperty)stmt.getProperty("rdfSubject")).toString(), 
						vFactory), 
					NTriplesUtil.parseURI(
						((IProperty)stmt.getProperty("rdfPredicate")).toString(),
						vFactory), 
					NTriplesUtil.parseValue(
						((IProperty)stmt.getProperty("rdfObject")).toString(), 
						vFactory), 
					NTriplesUtil.parseResource(
						((IProperty)stmt.getProperty("rdfContext")).toString(),
						vFactory)
				);
			} else {
				impl = new StatementImpl(
					NTriplesUtil.parseResource(
						((IProperty)stmt.getProperty("rdfSubject")).toString(), 
						vFactory), 
					NTriplesUtil.parseURI(
						((IProperty)stmt.getProperty("rdfPredicate")).toString(),
						vFactory), 
					NTriplesUtil.parseValue(
						((IProperty)stmt.getProperty("rdfObject")).toString(), 
						vFactory));
			}
		 
				
			stmts.add(impl);
			
			
		}

		agClient.addStatements(currentSession, stmts);

		ResultList rl = new ResultList();
		rl.setStatus(VitalStatus.withOK());
		return rl;
		
	}
	
	public VitalStatus deleteRDFStatements(VitalTransaction tx, List<GraphObject> rdfStatementList) throws Exception {
		
		List<Statement> stmts = new ArrayList<Statement>();
		
		AGraphSession currentSession = txSession(tx);
		
		for(GraphObject g : rdfStatementList) {
			
			if(!( g instanceof RDFStatement)) {
				throw new Exception("cannot mix rdf statements with other graph objects in allegrograph");
			}
			
			RDFStatement stmt = (RDFStatement)g;
			
			if( stmt.getProperty("rdfSubject") == null || stmt.getProperty("rdfPredicate") == null || stmt.getProperty("rdfObject") == null) {
				throw new Exception("Incomplete RDF statement: " + stmt);
			}
			
			StatementImpl impl = null;
			
			if(stmt.getProperty("rdfContext") != null) {
				impl = new ContextStatementImpl(
					NTriplesUtil.parseResource(
						((IProperty)stmt.getProperty("rdfSubject")).toString(), 
						vFactory), 
					NTriplesUtil.parseURI(
						((IProperty)stmt.getProperty("rdfPredicate")).toString(),
						vFactory), 
					NTriplesUtil.parseValue(
						((IProperty)stmt.getProperty("rdfObject")).toString(), 
						vFactory), 
					NTriplesUtil.parseResource(
						((IProperty)stmt.getProperty("rdfContext")).toString(),
						vFactory)
				);
			} else {
				impl = new StatementImpl(
					NTriplesUtil.parseResource(
						((IProperty)stmt.getProperty("rdfSubject")).toString(), 
						vFactory), 
					NTriplesUtil.parseURI(
						((IProperty)stmt.getProperty("rdfPredicate")).toString(),
						vFactory), 
					NTriplesUtil.parseValue(
						((IProperty)stmt.getProperty("rdfObject")).toString(), 
						vFactory));
			}
		 
				
			stmts.add(impl);
		}

		agClient.deleteStatements(currentSession, stmts);

		return VitalStatus.withOK();
		
	}

	
	/**
	 * Creates a new transaction object.
	 * An exception is thrown if an active open transaction already exists
	 * @throws Exception 
	 * 
	 */
	public String createTransaction() throws Exception {
	
		// if(currentSession != null) throw new RuntimeException("An active transaction detected, commit or rollback it first.");
		
		String sessionID = agClient.createSession();

		AGraphSession currentSession = new AGraphSession(sessionID);
		
		txMap.put(sessionID, currentSession);
		
		return currentSession.getSessionURL();
		
	}
	
	/**
	 * Commits an active transaction, state -> COMMITTED
	 * An exception is thrown if the transaction is inactive or some external
	 * error occured, state -> FAILED
	 * @param transaction
	 * @throws Exception 
	 * 
	 */
	public void commitTransation(String transactionID) throws Exception {
		
		AGraphSession currentSession = txMap.get(transactionID);
		
		if(currentSession == null) throw new RuntimeException("Transaction not found: " + transactionID);
		
		// if(!currentSession.getSessionURL().equals(transactionID)) throw new RuntimeException("Current transaction is different than provided: " + currentSession.getSessionURL() + " vs " + transactionID);

		agClient.commitTransaction(currentSession);
		
		txMap.remove(transactionID);
		
		// currentSession = null;
				
	}
	
	/**
	 * Rollbacks an active transaction, state -> ROLLED_BACK
	 * An exception is thrown if the transaction is inactive or some external 
	 * error occured, state -> FAILED
	 * @param transactionID
	 * @throws Exception 
	 * 
	 */
	public void rollbackTransaction( String transactionID ) throws Exception {
	
		AGraphSession currentSession = txMap.get(transactionID);
		
		if(currentSession == null) throw new RuntimeException("Transaction not found: " + transactionID);
		
		// if(!currentSession.getSessionURL().equals(transactionID)) throw new RuntimeException("Current transaction is different than provided: " + currentSession.getSessionURL() + " vs " + transactionID);
		
		agClient.rollbackTransaction(currentSession);
		
		txMap.remove(transactionID);
		
		// currentSession = null;
		
	}
	
	/**
	 * Returns a list of all open trasactions. at this moment 0 or 1
	 * @return
	 */
	public List<String> getTransactions() {
		// if(currentSession != null) return Arrays.asList(currentSession.getSessionURL());
		// return Collections.emptyList();
		List<String> l = new ArrayList<String>();
		synchronized (txMap) {
			l.addAll(txMap.keySet());
		}
		return l;
	}
	
	/**
	 * Sets current transaction. Not implemented.
	 * @return status
	 */
	public VitalStatus setTransaction(String transactionID) {
		return VitalStatus.withError("NOT IMPLEMENTED, use createTransaction");
	}
	
	
	public VITAL_GraphContainerObject getExistingObjects(List<VitalSegment> segmentsPool, List<String> uris) throws Exception {

		String directTypeTriplesQueryString = getDirectTypeTriplesQueryString(uris, toSegmentsURIs(segmentsPool));

		Map<String, String> segmentURI2ID = new HashMap<String, String>();
		
		for(VitalSegment s : segmentsPool) {
			
			String segmentID = (String) s.getRaw(Property_hasSegmentID.class);
			segmentURI2ID.put(s.getURI(), segmentID);
			
		}
				
		final List<BindingSet> results = new ArrayList<BindingSet>();
		
		
		SPARQLResultsJSONParser parser = new SPARQLResultsJSONParser();
		parser.setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
			
			@Override
			public void handleSolution(BindingSet bs)
					throws TupleQueryResultHandlerException {
				results.add(bs);
			}
			
		});
		agClient.sparqlSelectJsonOutput(directTypeTriplesQueryString, parser);
		
		VITAL_GraphContainerObject c = new VITAL_GraphContainerObject();
		c.setURI("urn:x");

		// return "SELECT ?u ?g ?o \n" + graphsString + " WHERE { GRAPH ?g { ?u " + p + " ?o . FILTER( ?u IN ( " + l.toString() + " ) ) } }";
		for(BindingSet bs : results) {
			
			URI segmentURI = (URI) bs.getValue("g");
			URI objURI = (URI) bs.getValue("u");
			String segmentID = segmentURI2ID.get(segmentURI.stringValue());
			c.setProperty(objURI.stringValue(), new StringProperty(segmentID));
			
		}
		
		return c;
		
	}
	
	/*
	public VitalStatus bulkExport(VitalOrganization organization, App app, VitalSegment segment, OutputStream outputStream)
			throws Exception {

		segment.setOrganizationID(organization.getID());
		segment.setAppID(app.getID());
		
		List<VitalSegment> segments = listSegments(organization, app);
		
		VitalSegment cSegment = null;
			
		for(VitalSegment s : segments) {
			if(s.getID().equals(segment.getID())) {
				cSegment = s;
			}
		}
			
		if(cSegment == null) throw new VitalServiceException("Segment not found: " + segment.getID() + ", app: " + app.getID() + ", organization: " + organization.getID());

		try {
			int exported = agClient.exportCompactString(toSegmentURI(segment), outputStream);
			VitalStatus status = VitalStatus.withOKMessage("Exported " + exported + " object(s)");
			status.setSuccesses(exported);
			return status;
		} catch(Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
		
	}
	*/
	
	public VitalStatus bulkExport(VitalSegment segment, OutputStream outputStream, String datasetURI)
			throws Exception {
		
		segment = checkSegment(segment);
		
		File dumpFile = null;
		File sortedFile = null;
		BufferedOutputStream os = null;
		
		VitalNTripleIterator iterator = null;
		
		try {

			dumpFile = File.createTempFile("agscan", ".nt");
			dumpFile.deleteOnExit();
			log.info("Dumping segment to temp file: " + dumpFile.getAbsolutePath() + " ...");
			os = new BufferedOutputStream(new FileOutputStream(dumpFile));
			
			String segmentURI = segment.getURI();
			
			long start = System.currentTimeMillis();
			
			// String sparql = "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <" + segmentURI + "> { ?s ?p ?o } } ORDER BY ASC(?s)";
			// agClient.sparqlGraphNTripleOutput(sparql, os);
			
			agClient.export(segmentURI, os);
			
			// conn.export(rdfHandler, conn.getValueFactory().createURI(segmentURI));
			
			// agClient.sparqlGraphQuery
			
			os.close();
			
			log.info("Dump complete, time: {}ms", System.currentTimeMillis() - start);
			log.info("Sorting output n-triples...");
			
			start = System.currentTimeMillis();
			
			sortedFile = File.createTempFile("agscansort", ".nt");
			
			ExternalSort.sort(dumpFile, sortedFile);
			
			FileUtils.deleteQuietly(dumpFile);
			dumpFile = null;
			
			log.info("Sort complete, time: {}ms", System.currentTimeMillis() - start);
			
			log.info("Iterating over ntriples blocks...");
			
			// sortedFile = dumpFile;

			
			BlockCompactStringSerializer writer = new BlockCompactStringSerializer(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
			
			int c = 0;
			
			int skipped = 0;
			
			for( iterator = new VitalNTripleIterator(sortedFile); iterator.hasNext(); ) {
				
				GraphObject g = iterator.next();
				
                if(datasetURI != null) {
                    String thisDataset = (String) g.getRaw(Property_hasProvenance.class);
                    if(thisDataset == null || !datasetURI.equals(thisDataset)) {
                        //skip the object
                        skipped++;
                        continue;
                    }
                }
				
				if(g == null) continue;
				
				writer.startBlock();
				writer.writeGraphObject(g);
				writer.endBlock();
				
				c++;
				
			}

			writer.flush();
			
			VitalStatus status = VitalStatus.withOKMessage("Exported " + c + " object(s)" + (datasetURI != null ? (", filtered out " + skipped + " object(s)") : ""));
			status.setSuccesses(c);
			return status;
			
		} catch(Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		} finally {

			IOUtils.closeQuietly(iterator);	
		
			FileUtils.deleteQuietly(dumpFile);
			FileUtils.deleteQuietly(sortedFile);
		
			IOUtils.closeQuietly(os);
		
		}
		
	}	

	/**
	 * @param organization
	 * @param app
	 * @param segment
	 * @param inputStream - ntriples input stream
	 * @return
	 * @throws Exception
	 */
	public VitalStatus bulkImportNTriples(VitalSegment segment, InputStream inputStream, String datasetURI) throws Exception {

		segment = checkSegment(segment);
		
		try {
			agClient.addNTriples(segment.getURI(), inputStream);
			return VitalStatus.withOKMessage("AGLoad success");
		} catch(Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}	
	}
	
	public VitalStatus bulkImportBlockCompact(VitalSegment segment, InputStream inputStream, String datasetURI) throws Exception {
		
		segment = checkSegment(segment);
		
		try {
			int r = agClient.bulkImportBlockCompactStream(segment.getURI(), inputStream, datasetURI);
			VitalStatus vs = VitalStatus.withOKMessage("Inserted " + r + " object(s)");
			vs.setSuccesses(r);
			return vs;
		} catch(Exception e) {
			return VitalStatus.withError(e.getLocalizedMessage());
		}
	}

	public AGraphClient getAgClient() {
		return agClient;
	}

}

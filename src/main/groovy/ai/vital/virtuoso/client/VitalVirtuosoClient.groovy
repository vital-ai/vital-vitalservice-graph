package ai.vital.virtuoso.client

import ai.vital.virtuoso.GraphQueryImplementation
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalservice.query.VitalGraphQuery
import ai.vital.vitalservice.query.VitalSelectAggregationQuery
import ai.vital.vitalservice.query.VitalSelectQuery
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.model.GraphMatch
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VitalApp
import javax.xml.bind.DatatypeConverter
import com.hp.hpl.jena.datatypes.RDFDatatype
import com.hp.hpl.jena.query.*
import com.hp.hpl.jena.rdf.model.Literal
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.update.UpdateProcessor
import java.sql.Connection
import virtuoso.jdbc4.VirtuosoConnection
import virtuoso.jena.driver.*
import org.apache.commons.lang3.StringEscapeUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory

// TODO joining across graphs?

class VitalVirtuosoClient {
	
	private final static Logger log = LoggerFactory.getLogger(VitalVirtuosoClient.class)
	
	private VitalApp app = VitalApp.withId("haley-saas")
	
	private String serverName
	
	private String user
	
	private String password
	
	private String graphName
	
	private VirtGraph graph
	
	private int port = 1111
	
	public VitalVirtuosoClient(
		String serverName, 
		String user,
		String password,
		String graphName) {
		
		this.serverName = serverName
		this.user = user
		this.password = password
		this.graphName = graphName
		this.graph = new VirtGraph(graphName, "jdbc:virtuoso://${serverName}:${port}", user, password)
	}
	
	public VitalVirtuosoClient(
		String serverName,
		int port,
		String user,
		String password,
		String graphName) {
		
		this.serverName = serverName
		this.port = port
		this.user = user
		this.password = password
		this.graphName = graphName
		
		this.graph = new VirtGraph(graphName, "jdbc:virtuoso://${serverName}:${port}", user, password)
	}
	
	static String graphListQuery = """
SELECT  DISTINCT ?g 
   WHERE  { GRAPH ?g {?s ?p ?o} } 
ORDER BY  ?g
"""
		
	public List<String> getGraphList() {
		
		List<String> graphList = []
		
		Query sparql = QueryFactory.create(graphListQuery)
		
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparql, graph)
		
		ResultSet results = vqe.execSelect()
		
		while (results.hasNext()) {
			
			QuerySolution result = results.nextSolution()
			
			RDFNode resultNode = result.get("g")
			
			String graphName = resultNode.toString()
			
			graphList.add(graphName)
			
		}
		
		return graphList
		
	}
	
	// Note: in vitalservice, graphs were associated with segments
	// we may which to associate graphs with accounts or segment + account
	// with global data in segment (with empty account uri, or constant like "global")
	
	// or just keep that as segments as before but create segments on a per account basis
	
	
	// virtgraph is tied to a graph, which is used by default for crud operations
	// however a sparql insert can be used to specify a graph instead of the default
	// String insertQuery = "INSERT DATA { GRAPH <http://example.org/otherGraph> { <http://example.org/subject> <http://example.org/predicate> <http://example.org/object> . } }";

	// add graph
	
	// remove graph
	
	
	// insert triples (list of string?)
	
	// delete statements/triples
	
	// bulk export, buffered output
	
	// bulk import, buffered input
	
	
	// ping, test connection
	
	
	// create transaction
	// rollback transaction
	// commit transaction
	
	// VirtuosoConnection conn = (VirtuosoConnection) graph.getConnection()

	
	
	// sparql ask: return true/false
	
	// sparql update: return void
	
	
	/////////////////////////////////////////////////////////////////
	// temp put crud operations for graph objects here
	// but this should be moved to VitalGraphService
	// these temp methods to be tied to the "current" graph only
	

	///////////////////////////////////////////////
	
	// delete graph object

	boolean deleteGraphObject(String uriString) {
		
		boolean exists = graphObjectExists(uriString)
		
		if(exists == false) {
			
			log.info("Attempt to delete object that does not exist")
			
			return false
		}
		
		List<String> tripleList = getTriples(uriString)
			
		if(tripleList.size() == 0) {
			
			log.info("Attempt to delete object with no triples")
			
			return false
		}
		
		String tripleString = ""
		
		for(t in tripleList) {
			
			tripleString = tripleString + t + "\n"
		}
		
		String sparqlDelete = """
DELETE DATA {
	GRAPH <${graphName}> {
${tripleString}				
	}
}
"""

		log.info( "DeleteSparql:\n" + sparqlDelete)
		
		try {
			
			VirtuosoUpdateRequest updateRequest = VirtuosoUpdateFactory.create(sparqlDelete, graph)
			
			updateRequest.exec()
			
		} catch(Exception ex) {
				
			ex.printStackTrace()
				
			return false
		}
		
		return true
	}
	
	// check if exists
	// get triples of object
	// delete triples
	
	
	// delete without getting triples
	
	///////////////////////////////////////////////
	// check if it exists
	// SPARQL DELETE { ?s ?p ?o }
	// WHERE { ?s ?p ?o . filter ( ?s = <uri>) };

		
	///////////////////////////////////////////////
	
	// insert graph object
	// check if exists, must not
	// convert object into triples
	// insert triples
	
	public boolean insertGraphObject(GraphObject go) {
		
		String uriString = go.URI
		
		boolean exists = graphObjectExists(uriString)
		
		if(exists == true) {
			
			log.info("Attempt to insert object that already exists")
			
			return false
		}
 		
		Model m = ModelFactory.createDefaultModel()
		
		go.toRDF(m)
		
		Long tripleCount = m.size()
	
		log.info( "Triple Count: " + tripleCount)
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		m.write(bos, "N-TRIPLE");
		
		String modelAsString = new String(bos.toByteArray());
		
		log.info( "Triples:\n" + modelAsString)
		
		String sparqlInsert = """
INSERT DATA {
GRAPH <${graphName}> {
${modelAsString}
}
}
"""

		log.info( "SPARQL:\n" + sparqlInsert )
		
		try {
		
			VirtuosoUpdateRequest updateRequest = VirtuosoUpdateFactory.create(sparqlInsert, graph)
		
			updateRequest.exec()
		
		} catch(Exception ex) {
			
			ex.printStackTrace()
			
			return false
		}
		
		return true
	}

	///////////////////////////////////////////////
	
	// update graph object
	// check if exists, must
	
	// get current triples of object
	
	// validate to make sure new version has valid triples
	
	// convert new object into triples
	
	// delete old triples
	// insert new triples
	
	
	
	public boolean updateGraphObject(GraphObject go) {
		
		String uriString = go.URI
		
		boolean exists = graphObjectExists(uriString)
		
		if(exists == false) {
			
			log.info("Attempt to update object that does not exist.")
			
			return false
		}
		
		
		boolean deleted = deleteGraphObject(go.URI)
		
		if(deleted == false) {
			
			log.info("Attempt to update object that failed to be removed.")
		
			return false
			
		}
		
		boolean inserted = insertGraphObject(go)
		
		if(inserted == false) {
			
			log.info("Attempt to update object that was removed but failed to be inserted.")
			
			return false
			
		}
		
		return true
	}
	
	///////////////////////////////////////////////
	// check if exists
	
	
	public boolean graphObjectExists(String uriString) {
		
		VitalSigns vs = VitalSigns.get()
		
		String goSparql = "SELECT ?s ?o WHERE { { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o } \nFILTER (\n?s in (<${uriString}>))}"
		
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(goSparql, graph)
				
		ResultSet results = vqe.execSelect()
			
		
		boolean found = false
		
		String objectTypeURI = null
		
		while (results.hasNext()) {
			
			QuerySolution result = results.nextSolution()
			
			RDFNode graph_name = result.get("graph")
			
			RDFNode subjectNode = result.get("s")
						
			RDFNode objNode = result.get("o")
	
			String objValue
			
			found = true
			
			// does this handle all cases in use for datatypes?
			if( objNode.isLiteral() ) {
				
				Literal objLiteral = objNode.asLiteral()
				
				RDFDatatype datatype = objLiteral.getDatatype()
				
				String datatypeURI = datatype.getURI()
				
				// Note:
				// got error with internal double quotes not escaped
				
				String objStringValue = objNode.asLiteral().value.toString()
				
				objStringValue = objStringValue.replaceAll("\"", "")
				
				//objValue = "\"" + objNode.asLiteral().value.toString() + "\"" +
				// "^^<${datatypeURI}>"
				
				objValue = "\"" + objStringValue + "\"" +
				"^^<${datatypeURI}>"
							
			}
			else {
				
				objValue = "<" + objNode.asResource().toString() + ">"
			}
				
			objectTypeURI = objValue
				
			// println "$uriString <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> $objectTypeURI"
			
		}
			
		return found
		
	}
	
	
	public List<String> getTriples(String uriString) {
		
		VitalSigns vs = VitalSigns.get()
			
		String goSparql = "SELECT ?s ?p ?o WHERE { { ?s ?p ?o } \nFILTER (\n?s in (<${uriString}>))}"
				
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(goSparql, graph)
		
		ResultSet results = vqe.execSelect()
	
		List<String> tripleList = []
		
		while (results.hasNext()) {
			
			QuerySolution result = results.nextSolution()
			
			RDFNode graph_name = result.get("graph")
			
			RDFNode subjectNode = result.get("s")
			
			RDFNode predNode = result.get("p")
			
			RDFNode objNode = result.get("o")
	
			String objValue
			
			// does this handle all cases in use for datatypes?
			if( objNode.isLiteral() ) {
				
				Literal objLiteral = objNode.asLiteral()
				
				RDFDatatype datatype = objLiteral.getDatatype()
				
				String datatypeURI = datatype.getURI()
				
				// Note:
				// got error with internal double quotes not escaped
				
				String objStringValue = objNode.asLiteral().value.toString()
				
				// objStringValue = objStringValue.replaceAll("\"", "")
				
				// TODO test is this is a general fix
				// had an issue with value like A\C in a string
				String stringEscaped = StringEscapeUtils.escapeJava(objStringValue)
				
				
				//objValue = "\"" + objNode.asLiteral().value.toString() + "\"" +
				// "^^<${datatypeURI}>"
				
				// objValue = "\"" + objStringValue + "\"" + "^^<${datatypeURI}>"
					
				objValue = "\"" + stringEscaped + "\"" + "^^<${datatypeURI}>"
				
			}
			else {
				
				objValue = "<" + objNode.asResource().toString() + ">"
			}
				
			String tripleString =  "<${subjectNode}> <${predNode}> ${objValue} ."
							
			tripleList.add(tripleString)
		}
			
		
		return tripleList

	}
	
		
	
	/////////////////////////////////////////////////////////////////
	
	
	public String toSparql(VitalGraphQuery graphQuery) {
		
		String sparqlQuery = GraphQueryImplementation.toSparqlString(graphQuery)
		
		return sparqlQuery
	}
	
	public String toSparql(VitalSelectQuery selectQuery) {
	
		String sparqlQuery = GraphQueryImplementation.toSparqlString(selectQuery)
		
		return sparqlQuery
	}
	
	
	public String toSparql(VitalSelectAggregationQuery selectAggQuery) {
		
		String sparqlQuery = GraphQueryImplementation.toSparqlString(selectAggQuery)
			
		return sparqlQuery
	}

	public ResultList querySparql(String sparqlQuery) {
		
		// Query sparql = QueryFactory.create(sparqlQuery)
	
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparqlQuery, graph)

		Date then = new Date()
		
		ResultSet results = vqe.execSelect()
		
		Date now = new Date()
		
		long then_long = then.getTime()
		
		long now_long = now.getTime()
		
		long delta = now_long - then_long
		
		Double delta_s = (Double) delta / 1000.0d
		
		log.info( "Sparql Graph Delta (ms): " + delta )
		
		log.info( "Sparql Delta (s): " + delta_s )

		int i = 0
		
		ResultList resultList = new ResultList()
		
		List<QuerySolution> solutionList = []
		
		List<String> uriList = []
		
		while (results.hasNext()) {
			
			i++
			
			QuerySolution result = results.nextSolution()
			
			solutionList.add(result)
			
			Iterator<String> varNameIterator = result.varNames()
			
			for(String v in varNameIterator) {
				
				if(v == "graph") { continue }
				
				RDFNode resultNode = result.get(v)
				
				// should not be any anonymous cases
				if( resultNode.isAnon() ) {
					
					continue
				}
				
				// primarily be this case
				if(resultNode.isURIResource()) {
					
					String resultURI = resultNode.toString()
					
					uriList.add(resultURI)
				}
				
				
				if(resultNode.isLiteral()) {
					
					
					
				}
				
			}
		}
				
		for(QuerySolution qs in solutionList  )	{
		
			Iterator<String> varNameIterator = qs.varNames()
			
			GraphMatch gm = new GraphMatch().generateURI(app)
			
			for(String v in varNameIterator) {
				
				RDFNode resultNode = qs.get(v)
				
				// should not be any anonymous cases?
				if( resultNode.isAnon() ) {
					
					continue
				}
				
				if(resultNode.isURIResource()) {
					
					String resultURI = resultNode.toString()
					
					URI uri = new URI(resultURI)
										
					gm.setProperty(v, uri)		
				}
				
				if(resultNode.isLiteral()) {
					
					// handle score cases?
					Object valueObject = resultNode.asLiteral().value
					
					if(valueObject instanceof BigDecimal) {
						
						Double doubleObject = valueObject.doubleValue()
						
						valueObject = doubleObject
						
						
					}
					
					
					if(valueObject instanceof BigInteger) {
						
						Integer integerObject = valueObject.intValue()
						
						valueObject = integerObject	
					}
					
					
					
					gm.setProperty(v, valueObject)
				}
			}
			
			resultList.addResult(gm, 1.00)
		}
			
		return resultList
	}
	
	public ResultList query(VitalGraphQuery graphQuery) {
		
		boolean inlineObjects = graphQuery.getPayloads()
		
		List segments = graphQuery.getSegments()
		
		// check if matching graph name
		// for(s in segments) {
		//	println "Segment: " + s	
		// }
		
		
		GraphQueryImplementation impl = new GraphQueryImplementation(graphQuery, null)
		
		String sparqlQuery = impl.generateSparqlQuery()
		
		// String sparqlQuery = GraphQueryImplementation.toSparqlString(graphQuery)
		
		// println "Generated Sparql:\n" + sparqlQuery
		
		
		// don't use jena parser to allow virtuoso extensions to pass thru
		// actually the prefix is important:
		// PREFIX bif: <http://www.openlinksw.com/schemas/bif#> 

		Query sparql = QueryFactory.create(sparqlQuery)
	
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparql, graph)

		Date then = new Date()
		
		ResultSet results = vqe.execSelect()
		
		Date now = new Date()
		
		long then_long = then.getTime()
		
		long now_long = now.getTime()
		
		long delta = now_long - then_long
		
		Double delta_s = (Double) delta / 1000.0d
		
		log.info( "Sparql Graph Delta (ms): " + delta )
		
		log.info( "Sparql Delta (s): " + delta_s )

		int i = 0
		
		ResultList resultList = new ResultList()
		
		
		Integer limit = graphQuery.getLimit()
		
		Integer offset = graphQuery.getOffset()
		
		resultList.setBindingNames(impl.bindingNames)
		
	
		if(limit != null) { resultList.setLimit(limit) }
			
		if(offset != null) { resultList.setOffset(offset) }
		
		
	
		List<QuerySolution> solutionList = []
		
		List<String> uriList = []
		
		while (results.hasNext()) {
			
			i++
			
			QuerySolution result = results.nextSolution()
			
			solutionList.add(result)
			
			Iterator<String> varNameIterator = result.varNames()
			
			for(String v in varNameIterator) {
				
				if(v == "graph") { continue }
				
				RDFNode resultNode = result.get(v)
				
				// should not be any anonymous cases
				if( resultNode.isAnon() ) {
					
					continue
				}
				
				// primarily be this case
				if(resultNode.isURIResource()) {
					
					String resultURI = resultNode.toString()
					
					uriList.add(resultURI) 	
				}
				
				// only for provides cases?
				if(resultNode.isLiteral()) {
					
					
					
				}
				
			}				
		}
		
		Map<String,GraphObject> objectMap = [:]
		
		if(inlineObjects) {
			
			List<GraphObject> objectList = resolveURIList(uriList)
			
			for(g in objectList) {
				
				String objectURI = g.URI
				
				objectMap[objectURI] = g
				
			}
		}	
		
		Map<String,Boolean> visitedMap = [:]
		
		for(QuerySolution qs in solutionList  )	{
		
			Iterator<String> varNameIterator = qs.varNames()
			
			GraphMatch gm = new GraphMatch().generateURI(app)
			
			for(String v in varNameIterator) {
				
				RDFNode resultNode = qs.get(v)
				
				// should not be any anonymous cases?
				if( resultNode.isAnon() ) {
					
					continue
				}
				
				// primarily be this case
				if(resultNode.isURIResource()) {
					
					String resultURI = resultNode.toString()
					
					URI uri = new URI(resultURI)
					
					// gm.setProperty(v, resultURI)
					
					gm.setProperty(v, uri)
					
					if(inlineObjects && v != "graph") {
						
						Boolean visited = visitedMap[resultURI]
						
						if(visited == null) {
						
							GraphObject go = objectMap[ resultURI ]
						
							if(go != null) {
							
								gm.setProperty(resultURI, go.toCompactString())
								
								visitedMap[resultURI] = true
							}
						}
					}
				}
				
				// only for provides cases?
				if(resultNode.isLiteral()) {
					
					
				}
				
			}
			
			resultList.addResult(gm, 1.00)	
		}
			
		return resultList	
		
	}
	
	public ResultList query(VitalSelectAggregationQuery aggQuery) {
		
		List segments = aggQuery.getSegments()
		
		// check if matching graph name
		// for(s in segments) {
		//	println "Segment: " + s
		// }
		
		String sparqlQuery = GraphQueryImplementation.toSparqlString(aggQuery)
		
		// println "Generated Sparql:\n" + sparqlQuery
		
		Query sparql = QueryFactory.create(sparqlQuery)
	
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparql, graph)

		Date then = new Date()
		
		ResultSet results = vqe.execSelect()
		
		Date now = new Date()
		
		long then_long = then.getTime()
		
		long now_long = now.getTime()
		
		long delta = now_long - then_long
		
		Double delta_s = (Double) delta / 1000.0d
		
		log.info( "Sparql Agg Delta (ms): " + delta )
		
		log.info( "Sparql Agg Delta (s): " + delta_s )

		
		int i = 0
		
		ResultList resultList = new ResultList()
		
		List<QuerySolution> solutionList = []
		
		List<String> uriList = []
		
		while (results.hasNext()) {
			
			i++
			
			QuerySolution result = results.nextSolution()
			
			solutionList.add(result)
		}
		
		for(QuerySolution qs in solutionList  )	{
		
			Iterator<String> varNameIterator = qs.varNames()
			
			GraphMatch gm = new GraphMatch().generateURI(app)
			
			for(String v in varNameIterator) {
				
				RDFNode resultNode = qs.get(v)
				
				// should not be any anonymous cases?
				if( resultNode.isAnon() ) {
					
					continue
				}
				
				if(resultNode.isURIResource()) {
					
					String resultURI = resultNode.toString()
										
					gm.setProperty(v, resultURI)
					
	
				}
				
				if(resultNode.isLiteral()) {
					
					Object resultObject = resultNode.asLiteral().value
					
					if(resultObject instanceof BigDecimal) {
						
						resultObject = resultObject as Double
						
					}
					
					gm.setProperty(v, resultObject)
				}
			}
			
			resultList.addResult(gm, 1.00)
		}
			
		return resultList
	}
	
	public ResultList query(VitalSelectQuery selectQuery) {
		
		List segments = selectQuery.getSegments()
		
		// check if matching graph name
		// for(s in segments) {
		//	println "Segment: " + s
		// }
		
		String sparqlQuery = GraphQueryImplementation.toSparqlString(selectQuery)
		
		// log.info("SparqlQuery:\n" + sparqlQuery) 
		
		// Query sparql = QueryFactory.create(sparqlQuery)
		
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparqlQuery, graph)
		
		Date then = new Date()
		
		ResultSet results = vqe.execSelect()
		
		Date now = new Date()
		
		long then_long = then.getTime()
		
		long now_long = now.getTime()
		
		long delta = now_long - then_long
		
		Double delta_s = (Double) delta / 1000.0d
		
		// log.info( "Select Delta (ms): " + delta )
		
		// log.info( "Select Delta (s): " + delta_s )

		int i = 0
		
		ResultList resultList = new ResultList()
		
		List<QuerySolution> solutionList = []
		
		List<String> uriList = []
		
		while (results.hasNext()) {
			
			i++
			
			QuerySolution result = results.nextSolution()
			
			solutionList.add(result)
			
			Iterator<String> varNameIterator = result.varNames()
			
			for(String v in varNameIterator) {
					
				if(v == "graph") { continue }
				
				RDFNode resultNode = result.get(v)
				
				// log.info("VarName: " + v)
				
				// log.info("ResultNode: " + resultNode)
				
				if(selectQuery.isProjectionOnly()) {
					
					if(v == "count") {
					
						Object resultObject = resultNode.asLiteral().value
						
						if(resultObject instanceof Integer) {
							
							Integer count = resultObject
							
							resultList.totalResults = count
						}
					}
				}
			
				// should not be any anonymous cases
				if( resultNode.isAnon() ) {
					
					continue
				}
				
				// primarily be this case
				if(resultNode.isURIResource()) {
					
					String resultURI = resultNode.toString()
					
					uriList.add(resultURI)
				}
				
				// only for provides cases?
				if(resultNode.isLiteral()) {
					
					
					
				}
				
			}
		}
		
		Map<String,GraphObject> objectMap = [:]
		
		// can remove any that are already cached
			
		then = new Date()
		
		List<GraphObject> objectList = resolveURIList(uriList)
			
		now = new Date()
		
		
		then_long = then.getTime()
		
		now_long = now.getTime()
		
		delta = now_long - then_long
		
		delta_s = (Double) delta / 1000.0d
		
		// log.info( "Resolve Delta (ms): " + delta )
		
		// log.info(  "Resolve Delta (s): " + delta_s )

		for(g in objectList) {
				
			String objectURI = g.URI
				
			objectMap[objectURI] = g
				
		}
		
		for(QuerySolution qs in solutionList) {
		
			Iterator<String> varNameIterator = qs.varNames()
			
			for(String v in varNameIterator) {
				
				RDFNode resultNode = qs.get(v)
				
				// should not be any anonymous cases
				if( resultNode.isAnon() ) {
					
					continue
				}
				
				// primarily be this case
				if(resultNode.isURIResource()) {
					
					String resultURI = resultNode.toString()
					
					GraphObject g = objectMap[resultURI]
					
					if(g != null) {
					
						resultList.addResult(g, 1.0)
					}
				}
				
				// should not be any of these?
				if(resultNode.isLiteral()) {
						
				}	
			}
		}
		
		return resultList
		
	}
	
	// TODO
	// skip parsing and directly use the RDFNode objects with VitalSigns?
	
	public List<GraphObject> resolveURIList(List<String> goURIList) {
		
		VitalSigns vs = VitalSigns.get()
		
		// split into groups for large lists over a threshold (1000?)
		// splitting by 850 seems a sweet spot
		// perhaps a better sparql way to get set of subjects?
		// try big OR list?
		// big OR list tested & seems worse
		
		// List filterList = []
		
		// filter out graph URI, this shouldnt get here anyway
		
		// for(s in goURIList) {
			
		// 	if(s == "graph uri") { continue }
			
		// 	filterList.add(s)
		// }
		
		if(goURIList.size() > 1000) {
			
			List subListList = goURIList.collate(850)
			
			List results = []
			
			for(List subList in subListList) {
				
				List<GraphObject> subResults = resolveURIList(subList)
				
				results.addAll(subResults)	
			}
			
			return results
		}
		
		List<String> subjectURIList = []
		
		for(s in goURIList) {
			
		 	subjectURIList.add("<" + s + ">")
		}
		
		// List subjectList = []
		
		// for(s in filterList) {
			
			// if(s == "http://vital.ai/graph/harbor-garage-policy-1") { continue }
			
			// subjectList.add("(?s = <" + s + ">)")
		
		// }
		
		// String filterORList = subjectList.join(" || \n")
		
		String subjectList = "\n(\n" + subjectURIList.join(",\n") + "\n)"
		
		String goSparql = "SELECT ?s ?p ?o WHERE { { ?s ?p ?o } \nFILTER (\n?s in ${subjectList})}"
		
		// String goSparql = "SELECT ?s ?p ?o WHERE { { ?s ?p ?o } FILTER (\n${filterORList}\n)}"
		
		// println "Sparql: " + goSparql
		
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(goSparql, graph)
		
		ResultSet results = vqe.execSelect()
	
		List<String> tripleList = []
		
		while (results.hasNext()) {
			
			QuerySolution result = results.nextSolution()
			
			RDFNode graph_name = result.get("graph")
			
			RDFNode subjectNode = result.get("s")
			
			RDFNode predNode = result.get("p")
			
			RDFNode objNode = result.get("o")
	
			String objValue
			
			// does this handle all cases in use for datatypes?
			if( objNode.isLiteral() ) {
				
				Literal objLiteral = objNode.asLiteral()
				
				RDFDatatype datatype = objLiteral.getDatatype()
				
				String datatypeURI = datatype.getURI()
				
				// println "DatatypeURI: " + datatypeURI
				
				// temp try to parse date as some values got messed up
				// 202-01-01T05:00:00Z
				// which actually should be a valid date but gets a parsing
				// error in vitalsigns
				
				// if(datatypeURI == "http://www.w3.org/2001/XMLSchema#dateTime") {
					
					// String dateString = objNode.asLiteral().value.toString()
					
					// Date date = null
					
					// try {
						
						//date = DatatypeConverter.parseDateTime(dateString).getTime()
						
					//	objValue = "\"" + objNode.asLiteral().value.toString() + "\"" +
					//	"^^<${datatypeURI}>"
						
					// } catch(Exception e) {
						
						// println "DateParse Error: " + e.localizedMessage
						// println "DateString: " + dateString
						
						// temp override value until re-import
						// objValue = "\"" + "2020-01-01T05:00:00.000Z" + "\"" + 
						//	"^^<${datatypeURI}>"
					// }
				// }
				// else {
				
				// Note:
				// got error with internal double quotes not escaped
				
				String objStringValue = objNode.asLiteral().value.toString()
				
				// objStringValue = objStringValue.replaceAll("\"", "")
				
				objStringValue = escapeForNTriples(objStringValue)
				
				//objValue = "\"" + objNode.asLiteral().value.toString() + "\"" +
				// "^^<${datatypeURI}>"
				
				objValue = "\"" + objStringValue + "\"" +
				"^^<${datatypeURI}>"
			
				// }
			}
			else {
				
				objValue = "<" + objNode.asResource().toString() + ">"
			}
							
			String tripleString =  "<${subjectNode}> <${predNode}> ${objValue} ."
				
			// println tripleString
			
			tripleList.add(tripleString)
		}
			
		String tripleListString = tripleList.join("\n")
		
		List<GraphObject> goList = []
		
		try {
			
			// println tripleListString
			
			goList = vs.fromRDFGraph(tripleListString)
			
		} catch(Exception ex) {
			
			ex.printStackTrace()
			
			log.info(  "Unparsed Triples:\n" + tripleListString )
		}
		
		return goList	
	}
	
	String escapeForNTriples(String input) {

		// Escape Java style which deals with backslashes and quotes among others
		String escapedJava = StringEscapeUtils.escapeJava( input )
			
		// Now, unescape those that are valid in NTriples (like \t, \n, \r, etc.) back to their original form
		// Apache Commons doesn't have unescape for NTriples, so we manually handle common cases
		String unescapedNTriples = escapedJava.replaceAll('\\\\t', '\t')
											  .replaceAll('\\\\r', '\r')
											  .replaceAll('\\\\n', '\n')
											  .replaceAll('\\\\b', '\b')
											  .replaceAll('\\\\f', '\f')
											  .replaceAll('\\\\\'', '\'')
		
		return unescapedNTriples
	}
	
}

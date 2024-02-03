package ai.vital.service.graph

import ai.vital.virtuoso.client.VitalVirtuosoClient
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalservice.query.VitalGraphQuery
import ai.vital.vitalservice.query.VitalSelectQuery
import ai.vital.vitalservice.query.VitalSparqlQuery
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VITAL_GraphContainerObject
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalTransaction
import ai.vital.vitalsigns.model.property.URIProperty

class VitalGraphService {
	
	private static String systemGraphPostfix = "_systemgraph"
	
	private static Integer MAX_TRANSACTIONS = 10
	
	// select query impl?
	
	// test with wordnet
	
	// add script for bulk loading

	String endpoint
	String repositoryName
	String username
	String password
	
	
	VitalGraphServiceStatus currentStatus = new VitalGraphServiceStatus()
	
	
	// keep one main connection and N connections for transactions
	
	VitalVirtuosoClient primaryClient = null
	
	Map<String,VitalVirtuosoClient> transactionClientMap = [:]
	

	public VitalGraphService(String endpoint, String repositoryName, String username, String password, Boolean initialize) {
	
		this.endpoint = endpoint
		this.repositoryName = repositoryName
		this.username = username
		this.password = password
		
		
		currentStatus = new VitalGraphServiceStatus()
		
		currentStatus.statusType = VitalGraphServiceStatusType.NULL
		
		currentStatus.statusCode = null
		
		currentStatus.statusMessage = null
			
		if(initialize == true) {
			
			initialize()
			
		}
		else {
			
			connect()
			
		}
		
		
	}
	
	public VitalGraphServiceStatus getStatus() {
		
		return currentStatus
		
	}
	
	
	private void initialize() {
		
		synchronized(this) {
			
			
			// set currentStatus for success or failure
			
		}
		
		
	}
	
	private void connect() {
		
		synchronized(this) {
		
		
		
			// set currentStatus for success or failure
		
		}
		
		
	}
	
	private boolean purge() {
		
		// remove graphs which have repositoryName as prefix
		
		// remove system graph
		
		// return false if error, else return true
		// update currentStatus before returning false
		
		
		synchronized(this) {
		
		
		
		
		}
		
		
		return true
	}
	
	
	
	
	// crud operations using explicit GRAPH elements rather than using default GRAPH
	
	// default graph to be the system segment graph which must exist
	
	// don't allow modifying the system graph via CRUD operations
	
	
	// close
	
	
	public VitalGraphServiceStatus close() {
		
		
		boolean success = disconnect()
		
		
		return currentStatus
		
	}
	
	
	
	private boolean disconnect() {
		
		// rolls back any open transactions
		// disconnects all clients/connections
		
		return true
		
	}
	
	
	
	// ping
	
	// purge
	
	// use a "system" graph to keep track of which other graphs belong to this service?
	// like <name-of-service>-system
	
	// essentially same as system segment, but not fully implementing vitalservice
	
	// need to use vitalsegment so that it matches up to what's used in the query
	
	// replace segment * with current list of segments
	
	
	// each transaction has it's own client with its own connection
	// so the connection can be committed or rolled back
	
	
	// public GraphObject save(VitalTransaction transaction, VitalSegment targetSegment, GraphObject graphObject, List<VitalSegment> segmentsPool) throws Exception {

	
	// 	public VitalStatus delete(VitalTransaction tx, List<VitalSegment> segmentsPool, List<URIProperty> uris)

	
	// 	public GraphObject get(List<VitalSegment> segmentsPool, URIProperty uri) throws Exception {

	
	// 	public ResultList save(VitalTransaction tx, VitalSegment targetSegment, List<GraphObject> graphObjectsList, List<VitalSegment> segmentsPool) throws Exception {

	
	// 	public VitalStatus delete(VitalTransaction tx, List<VitalSegment> segmentsPool, URIProperty uri)

	// 	if(uri.get().startsWith(URIProperty.MATCH_ALL_PREFIX)) {
	
	
	// 	public VitalStatus deleteAll(VitalSegment targetSegment) throws Exception {

	// 	public List<GraphObject> getBatch(List<VitalSegment> segmentsPool, Collection<String> uris) throws Exception {

	
	// 	public List<String> listAllSegments() throws Exception {

	
	// 	public VitalSegment addSegment(VitalSegment segment) throws Exception {

	// public VitalStatus removeSegment(VitalSegment segment, boolean deleteData) throws Exception {
		
	
	// public void deleteBatch(VitalTransaction tx, String[] context, Set<String> uris) throws Exception {

	
	// public ResultList sparqlQuery(VitalSparqlQuery query) throws Exception {
	
	
	// public ResultList graphQuery(VitalGraphQuery query) throws Exception {

	
	// 	public ResultList selectQuery(VitalSelectQuery query) throws Exception {

	
	// 	public int getSegmentSize(VitalSegment segment) throws Exception {

	
	// public ResultList insertRDFStatements(VitalTransaction transaction, List<GraphObject> rdfStatementList) throws Exception {

	
	// 	public VitalStatus deleteRDFStatements(VitalTransaction tx, List<GraphObject> rdfStatementList) throws Exception {

	
	// public String createTransaction() throws Exception {
	
	
	// public void commitTransation(String transactionID) throws Exception {
	
	
	// public void rollbackTransaction( String transactionID ) throws Exception {
	
	
	// public List<String> getTransactions() {
		
	// 	public VITAL_GraphContainerObject getExistingObjects(List<VitalSegment> segmentsPool, List<String> uris) throws Exception {

	
	// public VitalStatus bulkExport(VitalSegment segment, OutputStream outputStream, String datasetURI)
	//		throws Exception {

	// public VitalStatus bulkImportNTriples(VitalSegment segment, InputStream inputStream, String datasetURI) throws Exception {

	
	// public VitalStatus bulkImportBlockCompact(VitalSegment segment, InputStream inputStream, String datasetURI) throws Exception {
	
	
	// rollback any open transactions, close connections
	
	// @Override
	// public void finalize() {
	// }
	
	
}

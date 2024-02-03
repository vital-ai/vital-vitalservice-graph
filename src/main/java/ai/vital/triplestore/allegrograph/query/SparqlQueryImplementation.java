package ai.vital.triplestore.allegrograph.query;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONParser;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.nquads.NQuadsParser;
import org.openrdf.rio.ntriples.NTriplesUtil;
import ai.vital.allegrograph.client.AGraphClient;
import ai.vital.triplestore.allegrograph.AllegrographWrapper;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalsigns.model.RDFStatement;
import ai.vital.vitalsigns.model.SparqlAskResponse;
import ai.vital.vitalsigns.uri.URIGenerator;

public class SparqlQueryImplementation {

	@SuppressWarnings("deprecation")
	public static ResultList handleSparqlQuery(AllegrographWrapper wrapper, AGraphClient client, VitalSparqlQuery sq) throws Exception {
		
		final ResultList rl = new ResultList();
		rl.setTotalResults(-1);
		
		String sparql = sq.getSparql();
		if(sparql == null || sparql.trim().isEmpty()) throw new RuntimeException("Sparql string cannot be null or empty");
		
		ParsedQuery parseQuery = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparql, null);
		
		if(parseQuery instanceof ParsedBooleanQuery) {
			
			boolean response = client.sparqlAskQuery(sparql);
			
			SparqlAskResponse askRes = new SparqlAskResponse();
			askRes.setURI(URIGenerator.generateURI(null, SparqlAskResponse.class, true));
			askRes.setProperty("positiveResponse", response);
			rl.getResults().add(new ResultElement(askRes, 1D));
			rl.setTotalResults(1);
			
		} else if( parseQuery instanceof ParsedGraphQuery ) {
			
			ParsedGraphQuery pgq = (ParsedGraphQuery) parseQuery;
			
			int bindingNamesCount = pgq.getTupleExpr().getAssuredBindingNames().size();
			
			// AGGraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, sparql);
			
			NQuadsParser parser = new NQuadsParser();
				
			parser.setRDFHandler(new RDFHandlerBase() {
								
				@Override
				public void handleStatement(Statement stmt) throws RDFHandlerException {
					
					RDFStatement r = new RDFStatement();
					r.setURI(URIGenerator.generateURI(null, RDFStatement.class, true));
					
					if( stmt.getContext() != null ) {
						r.setProperty("rdfContext", NTriplesUtil.toNTriplesString(stmt.getContext()));
					}
					
					r.setProperty("rdfSubject", NTriplesUtil.toNTriplesString(stmt.getSubject()));
					r.setProperty("rdfPredicate", NTriplesUtil.toNTriplesString(stmt.getPredicate()));
					r.setProperty("rdfObject", NTriplesUtil.toNTriplesString(stmt.getObject()));
				
					rl.getResults().add(new ResultElement(r, 1D));
				}
			});
			
			client.sparqlGraphNQuadsOutput(sparql, parser, bindingNamesCount == 4);
				
			rl.setTotalResults(rl.getResults().size());
				
		} else if(parseQuery instanceof ParsedTupleQuery) {
			
			SPARQLResultsJSONParser parser = new SPARQLResultsJSONParser();
			parser.setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
				
				@Override
				public void handleSolution(BindingSet bs)
						throws TupleQueryResultHandlerException {
					
					ai.vital.vitalsigns.model.SparqlBinding vb = new ai.vital.vitalsigns.model.SparqlBinding();
					vb.setURI(URIGenerator.generateURI(null, ai.vital.vitalsigns.model.SparqlBinding.class, true));
					
					for(Binding b : bs) {
						Value v = b.getValue();
						String ntv = null;
						if( v != null) {
							ntv = NTriplesUtil.toNTriplesString(v);
						} else {
							ntv = "UNBOUND";
						}
						vb.setProperty(b.getName(), ntv);
					}
					
					rl.getResults().add(new ResultElement(vb, 1D));
					
				}
				
			});
			
			client.sparqlSelectJsonOutput(sparql, parser);
			
			rl.setTotalResults(rl.getResults().size());
				
		} else {
			throw new RuntimeException("Unsupported sparql query type: " + parseQuery.getClass().getSimpleName());
		}
		
		// Query q = conn.prepareQuery(QueryLanguage.SPARQL, sparql);
		
		return rl;
		
	}
}

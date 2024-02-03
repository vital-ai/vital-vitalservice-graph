package ai.vital.triplestore.allegrograph.query;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;

import ai.vital.vitalsigns.uri.URIGenerator;
import ai.vital.vitalsigns.model.AggregationResult;
import ai.vital.triplestore.sparql.SparqlQueryGenerator;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.model.GraphObject;

public class SelectQueryImplementation {

	/*
	public static ResultList handleSelectQuery(AllegrographWrapper wrapper, AGRepositoryConnection conn, VitalSelectQuery sq) throws Exception {
		
		List<String> allResults = new ArrayList<String>();
		
		List<String> contexts = new ArrayList<String>();
		
		for(VitalSegment s : sq.getSegments()) {
			contexts.add(wrapper.toSegmentURI(s));
		}
		
		AggregationType aggType = null;
		
		if(sq instanceof VitalSelectAggregationQuery) {
			VitalSelectAggregationQuery vsaq = (VitalSelectAggregationQuery)sq;
			aggType = vsaq.getAggregationType();
			String propertyURI = vsaq.getPropertyURI();
			if(propertyURI == null) {
				if(aggType != AggregationType.count) throw new RuntimeException("Aggregation type " + aggType.name() + " requires vital property");
			}
			
		}
				
				
		
		String sparql = SparqlQueryGenerator.selectQuery(sq, contexts);
		
		ResultList rl = new ResultList();
		
		if(sq.getReturnSparqlString()) {
			rl.setStatus(VitalStatus.withOKMessage(sparql));
			return rl;
		}
		
		TupleQueryResult result = null;
		
		
		Double aggValue = null;
		
		try {
			
			result = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql).evaluate();
			
			if(aggType == null) {
				
				while(result.hasNext()) {
					BindingSet next = result.next();
					for(String n : next.getBindingNames()) {
						Value v = next.getValue(n);
						if(v instanceof URI) {
							String uri = v.stringValue();
							if(!allResults.contains(uri))allResults.add(uri);
						}
					}
				}
				
			} else if(
					aggType == AggregationType.count || 
					aggType == AggregationType.sum ||
					aggType == AggregationType.average ||
					aggType == AggregationType.min ||
					aggType == AggregationType.max
			){
				
				Literal literalValue = (Literal) result.next().getBinding(aggType.name()).getValue();
				aggValue = literalValue.doubleValue();
				
			} else {
				throw new RuntimeException("Unhandled aggregation type: " + aggType);
			}
			
			
		} finally {
			if(result != null) try {result.close();}catch(Exception e){}
		}

		
		if(aggType == null) {
			
			int limit = sq.getLimit();
			int offset = sq.getOffset();
			
			rl.setTotalResults(allResults.size());
			rl.setLimit(limit);
			rl.setOffset(offset);
			
			List<String> toCollect = null;
			
			if(offset < allResults.size()) {
				toCollect = allResults.subList(offset, Math.min(allResults.size(), offset + limit));
			}
			
			if(toCollect != null && toCollect.size() > 0) {
				
				for(GraphObject g : wrapper._getBatch(conn, contexts.toArray(new String[contexts.size()]), toCollect)) {
					
					if(g != null) {
						
						rl.getResults().add(new ResultElement(g, 1.0d));
						
					}
					
				}
				
			}
			
		} else {
			
			AggregationResult aggRes = new AggregationResult();
			aggRes.setURI(URIGenerator.generateURI(null, AggregationResult.class, true));
			aggRes.setProperty("aggregationType", aggType.name());
			aggRes.setProperty("value", aggValue);
			
			rl.getResults().add(new ResultElement(aggRes, 1D));
			
		}
		
		
		
		return rl;
	}

	*/
	
}

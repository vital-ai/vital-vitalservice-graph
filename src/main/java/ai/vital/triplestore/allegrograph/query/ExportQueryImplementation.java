package ai.vital.triplestore.allegrograph.query;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONParser;

import ai.vital.allegrograph.client.AGraphClient;
import ai.vital.triplestore.allegrograph.AllegrographWrapper;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExportQuery;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalSegment;

public class ExportQueryImplementation {

	@SuppressWarnings("deprecation")
	public static ResultList handleExportQuery(AllegrographWrapper wrapper, AGraphClient client, VitalExportQuery exportQuery, QueryStats queryStats) throws Exception {
		
		List<String> contexts = new ArrayList<String>();
		
		long start = System.currentTimeMillis();
		
		String[] segURIs = new String[exportQuery.getSegments().size()];

		int i = 0;
		for(VitalSegment s : exportQuery.getSegments()) {
			String segURI = s.getURI();
			contexts.add(segURI);
			segURIs[i++] = segURI;
		}
		
		String g = "";
		if(contexts != null) {
			for(String ctx : contexts) {
				g += "FROM <" + ctx + "> \n";
			}
		}
		
		
		String sparql = "SELECT distinct ?s \n " + g +  " where { ?s ?p ?o } ORDER BY ASC(?s) OFFSET " + exportQuery.getOffset() + " LIMIT " + exportQuery.getLimit();
		
		
		SPARQLResultsJSONParser parser = new SPARQLResultsJSONParser();
		
		final List<String> uris = new ArrayList<String>();
		
		parser.setTupleQueryResultHandler(new TupleQueryResultHandlerBase(){

			@Override
			public void handleSolution(BindingSet bindingSet)
					throws TupleQueryResultHandlerException {
				Value value = bindingSet.getValue("s");
				if(value instanceof URI) {
					String u = ((URI)value).stringValue();
					if(!uris.contains(u)) uris.add(u);
				}

			}
			
		});
		client.sparqlSelectJsonOutput(sparql, parser);
		
		
		ResultList rl = new ResultList();
		rl.setLimit(exportQuery.getLimit());
		rl.setOffset(exportQuery.getOffset());
		
		
		if(uris.size() > 0 ) {
			
			for(GraphObject go : wrapper._getBatch(segURIs, uris, queryStats)) {
				
				rl.getResults().add(new ResultElement(go, 1d));
				
			}
			
		}
		
		rl.setQueryStats(queryStats);
		if(queryStats != null) {
			queryStats.setQueryTimeMS(System.currentTimeMillis() - start);
		}
		
		
		return rl;
		
		
	}
	
}

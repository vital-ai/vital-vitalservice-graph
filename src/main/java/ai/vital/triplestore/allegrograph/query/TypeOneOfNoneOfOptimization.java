package ai.vital.triplestore.allegrograph.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryResultHandler;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.allegrograph.client.AGraphClient;
import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.QueryTime;
import ai.vital.vitalservice.query.VitalGraphArcContainer;
import ai.vital.vitalservice.query.VitalGraphBooleanContainer;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalGraphQueryContainer;
import ai.vital.vitalservice.query.VitalGraphQueryElement;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion.Comparator;
import ai.vital.vitalservice.query.VitalGraphQueryTypeCriterion;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.classes.ClassMetadata;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Edge;
import ai.vital.vitalsigns.model.VITAL_HyperEdge;
import ai.vital.vitalsigns.model.VITAL_HyperNode;
import ai.vital.vitalsigns.model.VITAL_Node;
import ai.vital.vitalsigns.model.VitalSegment;

public class TypeOneOfNoneOfOptimization {

	private final static Logger log = LoggerFactory.getLogger(TypeOneOfNoneOfOptimization.class);
	
	static List<Class<? extends GraphObject>> baseClasses = Arrays.asList(VITAL_Node.class, VITAL_Edge.class, VITAL_HyperEdge.class, VITAL_HyperNode.class);

	Map<Class<? extends GraphObject>, Set<ClassMetadata>> baseClass2Types = null;

	private AGraphClient agClient;

	private List<VitalSegment> segments;

	private QueryStats queryStats;

	public TypeOneOfNoneOfOptimization(AGraphClient agClient, List<VitalSegment> segments, QueryStats stats) throws Exception {
		this.agClient = agClient;
		this.segments = segments;
		this.queryStats = stats;
		
	}
	
	protected void initTypes() throws Exception {
		
		if(baseClass2Types != null) return;
		
		baseClass2Types = new HashMap<Class<? extends GraphObject>, Set<ClassMetadata>>();
		
		String sparql = "SELECT DISTINCT ?type\n";
		
		for(VitalSegment vs : segments) {
			sparql += "FROM <" + vs.getURI() + ">\n";
		}

		sparql += " WHERE { ?s a ?type }";
		
		final Set<String> availableTypes = new HashSet<String>();
		
		SPARQLResultsJSONParser parser = new SPARQLResultsJSONParser();
		parser.setQueryResultHandler(new QueryResultHandler() {
			
			@Override
			public void startQueryResult(List<String> arg0) throws TupleQueryResultHandlerException {
				
			}
			
			@Override
			public void handleSolution(BindingSet arg0) throws TupleQueryResultHandlerException {
				Binding binding = arg0.getBinding("type");
				if(binding != null) {
					availableTypes.add(binding.getValue().stringValue());
				}
			}
			
			@Override
			public void handleLinks(List<String> arg0) throws QueryResultHandlerException {
				
			}
			
			@Override
			public void handleBoolean(boolean arg0) throws QueryResultHandlerException {
				
			}
			
			@Override
			public void endQueryResult() throws TupleQueryResultHandlerException {
				
			}
		});
		
		long start = System.currentTimeMillis();
		agClient.sparqlSelectJsonOutput(sparql, parser);
		if(queryStats != null) {
			if( queryStats.getQueriesTimes() != null ) {
				queryStats.getQueriesTimes().add(new QueryTime("Types from segments query", sparql, System.currentTimeMillis() - start));
			}
		}
		if(log.isDebugEnabled()) {
			log.debug("Types qtime: " + ( System.currentTimeMillis() - start) + "ms");
		}
		
		
		for(String t : availableTypes) {
			
			ClassMetadata cm = VitalSigns.get().getClassesRegistry().getClass(t);
			if(cm == null) continue;
			
			Class<? extends GraphObject> c = null;
			
			for(Class<? extends GraphObject> bc : baseClasses) {
				
				if(bc.isAssignableFrom(cm.getClazz())) {
					c = bc;
					break;
				}
				
			}
			
			if(c == null) continue;
			
			Set<ClassMetadata> set = baseClass2Types.get(c);
			if(set == null) {
				set = new HashSet<ClassMetadata>();
				baseClass2Types.put(c, set);
			}
			
			set.add(cm);
			
		} 
		
		
	}
	
	public void processContainer(VitalGraphCriteriaContainer parent, VitalGraphCriteriaContainer container) throws Exception {
		
		List<VitalGraphQueryTypeCriterion> tcs = new ArrayList<VitalGraphQueryTypeCriterion>();
		
		int nonTC = 0;
		
		for(VitalGraphQueryElement el : new ArrayList<VitalGraphQueryElement>(container)) {
			
			if(el instanceof VitalGraphCriteriaContainer) {
				processContainer(container, (VitalGraphCriteriaContainer) el);
				nonTC++;
			} else if(el instanceof VitalGraphQueryTypeCriterion) {
				VitalGraphQueryTypeCriterion el2 = (VitalGraphQueryTypeCriterion) el;
				Class<? extends GraphObject> gType = el2.getType();
				if(gType == null) {
					nonTC++;
					continue;
				}
				
				if(el2.getComparator() == Comparator.EQ || el2.getComparator() == Comparator.NE) {
					tcs.add(el2);
				} else {
					nonTC++;
				}
			} else {
				nonTC++;
			}
			
		}
		
//		if(parent == null) return;
		
		if(nonTC > 0 || tcs.size() == 0) return;
		
//		if(tcs.size() == 1) {
//
//			//unwrap it
//			parent.remove(container);
//			parent.add(tcs.get(0));
//			
//			return;
//		} 

		
		//merge
		List<String> types = new ArrayList<String>();
		
		Class<? extends GraphObject> baseClass = null;
		
		for(VitalGraphQueryTypeCriterion t : tcs) {
			
			Class<? extends GraphObject> gType = t.getType();
			
			if(gType == null) throw new RuntimeException("No class set in type criterion" + t);
			
			ClassMetadata cm = VitalSigns.get().getClassesRegistry().getClassMetadata(gType);
			if(cm == null) throw new RuntimeException("Class metadata not found for type: " + gType.getCanonicalName());
			
			Class<? extends GraphObject> clz = cm.getClazz();
			
			Class<? extends GraphObject> newBaseClass = null;
			
			for(Class<? extends GraphObject> bc : baseClasses) {
				if(bc.isAssignableFrom(clz)) {
					newBaseClass = bc;
				}
			}
			
			if(newBaseClass == null) throw new RuntimeException("No base graph object type found for class: " + clz);
			
			if(baseClass == null) {
				baseClass = newBaseClass;
			} else {
				if(!baseClass.equals(newBaseClass)) {
//					throw new RuntimeException("Type constraint of different types in a single container detected: " + baseClass + " vs. " + newBaseClass);
					//don't optimize it, the types should be grouped!
					return;
				}
			}
			
			types.add(cm.getURI());
			
		}
		
		initTypes();
		
		Set<ClassMetadata> availableTypes = baseClass2Types.get(baseClass);
		if(availableTypes == null || availableTypes.size() == 0) {
			return;
		}
		
		List<String> complementaryList = new ArrayList<String>(availableTypes.size());
		Map<String, ClassMetadata> complementaryMap = new HashMap<String, ClassMetadata>();
		for(ClassMetadata cm : availableTypes) {
			complementaryList.add(cm.getURI());
			complementaryMap.put(cm.getURI(), cm);
		}
		complementaryList.removeAll(types);
		for(String t : types) {
			complementaryMap.remove(t);
		}
		
		//flip the container
		boolean initialAnd = container.getType() == QueryContainerType.and;
		
		
		double inputOutputRation = initialAnd ? 6d : 1d/6d;
		
		if(initialAnd) {
			
			
			
		}
		
		if(complementaryList.size() > 0 && types.size() * inputOutputRation > complementaryList.size() ) {
			
			container.clear();

			container.setType(initialAnd ? QueryContainerType.or : QueryContainerType.and);
			
			for(String t : complementaryList) {
				
				VitalGraphQueryTypeCriterion tt = new VitalGraphQueryTypeCriterion(tcs.get(0).getSymbol(), complementaryMap.get(t).getClazz());
				tt.setComparator(initialAnd ? Comparator.EQ : Comparator.NE);
				container.add(tt);
			}
			
		}
		
		
	}

	public void processQuery(VitalQuery query) throws Exception {

		if(query instanceof VitalSelectQuery ) {

			//don't process it!
			
//			processContainer(null, ((VitalSelectQuery) query).getCriteriaContainer());
			
		} else if(query instanceof VitalGraphQuery) {
			
			VitalGraphQuery vgq = (VitalGraphQuery) query;
			
			processArcContainer(null, vgq.getTopContainer());
			
		}
		
		
	}

	private void processArcContainer(VitalGraphArcContainer parentArc, VitalGraphArcContainer container) throws Exception {


		for(VitalGraphQueryContainer<?> c : container) {
			if(c instanceof VitalGraphArcContainer) {
				processArcContainer(container, (VitalGraphArcContainer) c);
			} else if(c instanceof VitalGraphBooleanContainer) {
				processBooleanContainer(container, (VitalGraphBooleanContainer) c);
			} else if(c instanceof VitalGraphCriteriaContainer) {
				if(parentArc == null) {
					//cannot optimize root container
					return;
				}
				processContainer(null, (VitalGraphCriteriaContainer) c);
			} else {
				throw new RuntimeException("Unexpected arc container child: " + c);
			}
			
		}
		
		
	}

	private void processBooleanContainer(VitalGraphArcContainer parentArc, VitalGraphBooleanContainer c) throws Exception {

		for(VitalGraphQueryContainer<?> ch : c) {
			if(ch instanceof VitalGraphArcContainer) {
				processArcContainer(parentArc, (VitalGraphArcContainer) ch);
			} else if(ch instanceof VitalGraphBooleanContainer) {
				processBooleanContainer(parentArc, (VitalGraphBooleanContainer) ch);
			} else {
				throw new RuntimeException("Unexpected boolean container child: " + c);
			}
		}
		
	}
	
}


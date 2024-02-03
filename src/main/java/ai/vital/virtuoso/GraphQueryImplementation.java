package ai.vital.virtuoso;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.vital.domain.ontology.VitalOntology;
import ai.vital.virtuoso.sparql.SparqlQueryGenerator;
import ai.vital.virtuoso.sparql.model.RDFSerialization;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.AggregationType;
import ai.vital.vitalservice.query.Connector;
import ai.vital.vitalservice.query.Destination;
import ai.vital.vitalservice.query.GraphElement;
import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.QueryStats;
import ai.vital.vitalservice.query.QueryTime;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.Source;
import ai.vital.vitalservice.query.VitalGraphArcContainer;
import ai.vital.vitalservice.query.VitalGraphArcContainer.Capture;
import ai.vital.vitalservice.query.VitalGraphArcElement;
import ai.vital.vitalservice.query.VitalGraphBooleanContainer;
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalGraphQueryContainer;
import ai.vital.vitalservice.query.VitalGraphQueryElement;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion;
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion.Comparator;
import ai.vital.vitalservice.query.VitalGraphQueryTypeCriterion;
import ai.vital.vitalservice.query.VitalGraphValue;
import ai.vital.vitalservice.query.VitalGraphValueCriterion;
import ai.vital.vitalservice.query.VitalQuery;
import ai.vital.vitalservice.query.VitalSelectAggregationQuery;
import ai.vital.vitalservice.query.VitalSelectQuery;
import ai.vital.vitalservice.query.VitalSortProperty;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.classes.ClassMetadata;
import ai.vital.vitalsigns.datatype.Truth;
import ai.vital.vitalsigns.model.AggregationResult;
import ai.vital.vitalsigns.model.GraphMatch;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.model.property.StringProperty;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;
import ai.vital.vitalsigns.properties.PropertyMetadata;
import ai.vital.vitalsigns.rdf.RDFSerialization.RDFVitalPropertyFilter;
import ai.vital.vitalsigns.uri.URIGenerator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.XSD;

// TODO

// Notes:
// for Aggregation
// the implementation was initially for a flat select of objects
// which would have one property defined to be the input to the aggregation
// with that property associated with the object in the flat set.

// with graph functions, the input to the aggregation may occur within
// any part of the graph

// longer term, the query should not just accept a property URI to aggregate
// but instead mark the ARC and property to use for the aggregation
// potentially using the "provides" function

// to test graph queries with aggregation, modifications were made
// to skip inserting the aggregation property criteria into the query
// except if there is a query criterion from that property property
// declared in the query

// this would break if the same property appeared more than once in the query
// such as the "doubleProperty" appearing once for the aggregation case
// and other times for some other value, like:
// average (height) for (weight > 200)
// with these being both double properties such as as in answer instance objects

// and this requires injecting the aggregation property into the query
// like injecting salary > 0 in order to find average salary

// a workaround could be to include some additional info into in the query
// to indicate which is the real aggregation one


// added a partial work around to use:
// value countArc: false
// to mark an ARC as the aggregation one
// this was used because it's a field in the ARC object that not being used otherwise
// but it defaults to true, thus needing to mark with "false" value


// TODO
// test with query with same property name used in different ARCs
// make sure data structure accounts for multiple property names like
// hasName that could appear multiple times in different ARCS
// these should get distinct ?value names which could have separate constraints

// TODO test using exist comparable to inject a constraint when using agg function
// test with URI exists for count and count_distinct cases

public class GraphQueryImplementation {

	public static abstract class FilterContainer {
	
		public FilterContainer(FilterContainer parent) {

			this.parent = parent;
			
			if(parent != null ) {
				parent.containers.add(this);
			}	
		}
		
		FilterContainer parent;
		
		List<String> filters = new ArrayList<String>();
		
		List<FilterContainer> containers = new ArrayList<FilterContainer>();

	}
	
	public static class FilterAND extends FilterContainer{

		public FilterAND(FilterContainer parent) {
			super(parent);
		}	
	}
	
	// all unions mapped to giant OR
	public static class FilterOR extends FilterContainer {

		public FilterOR(FilterContainer parent) {
			super(parent);
		}
	}
	
	private final static Logger log = LoggerFactory.getLogger(GraphQueryImplementation.class);
	
	private final static String ind = "  ";
	
	// context
	int counter = 0;
	
	int boundCounter = 0;
	
	int providesCounter = 0;

	// for select + aggregation/distinct case
	String overriddenBindingsString = null;
	List<String> bindings = new ArrayList<String>();
	
	// capture the var name for the generated agg function
	String aggregationVarName = null;
	
	// for results
	List<String> bindingNames = new ArrayList<String>();
	
	StringBuilder builder = new StringBuilder();
	
	// use giant filter builder 
	FilterContainer topFilterContainer = new FilterAND(null);
	
	int valueIndex = 0;

	int predicateIndex = 0;
	
	private VitalQuery query;
	
	private VitalGraphQuery graphQuery;
	
	static List<VitalGraphArcElement> validArcs = new ArrayList<VitalGraphArcElement>();
	
	// root cases 
	public final static VitalGraphArcElement CURRENT_EMPTY_EMPTY = new VitalGraphArcElement(Source.CURRENT, Connector.EMPTY, Destination.EMPTY);
	
	public final static VitalGraphArcElement EMPTY_EDGE_EMPTY = new VitalGraphArcElement(Source.EMPTY, Connector.EDGE, Destination.EMPTY);
	
	public final static VitalGraphArcElement EMPTY_HYPEREDGE_EMPTY = new VitalGraphArcElement(Source.EMPTY, Connector.HYPEREDGE, Destination.EMPTY);
	
	static {
		validArcs.addAll(Arrays.asList(
			CURRENT_EMPTY_EMPTY,
			EMPTY_EDGE_EMPTY,
			EMPTY_HYPEREDGE_EMPTY
		));
	}
	
	// edge cases
	public final static VitalGraphArcElement PARENTSRC_EDGE_CURRENTDEST = new VitalGraphArcElement(Source.PARENT_SOURCE, Connector.EDGE, Destination.CURRENT);
	
	public final static VitalGraphArcElement PARENTDEST_EDGE_CURRENTDEST = new VitalGraphArcElement(Source.PARENT_DESTINATION, Connector.EDGE, Destination.CURRENT);
	
	public final static VitalGraphArcElement CURRENTSRC_EDGE_PARENTSRC = new VitalGraphArcElement(Source.CURRENT, Connector.EDGE, Destination.PARENT_SOURCE);
	
	public final static VitalGraphArcElement CURRENTSRC_EDGE_PARENTDESTINATION = new VitalGraphArcElement(Source.CURRENT, Connector.EDGE, Destination.PARENT_DESTINATION);

	
	static {
		validArcs.addAll(Arrays.asList(
			PARENTSRC_EDGE_CURRENTDEST,
			PARENTDEST_EDGE_CURRENTDEST,
			CURRENTSRC_EDGE_PARENTSRC,
			CURRENTSRC_EDGE_PARENTDESTINATION
		));
	}
	
	// hyper edge case
	public final static VitalGraphArcElement PARENTSRC_HYPEREDGE_CURRENTDEST = new VitalGraphArcElement(Source.PARENT_SOURCE, Connector.HYPEREDGE, Destination.CURRENT);
	
	public final static VitalGraphArcElement PARENTDEST_HYPEREDGE_CURRENTDEST = new VitalGraphArcElement(Source.PARENT_DESTINATION, Connector.HYPEREDGE, Destination.CURRENT);
	
	public final static VitalGraphArcElement CURRENTSRC_HYPEREDGE_PARENTSRC = new VitalGraphArcElement(Source.CURRENT, Connector.HYPEREDGE, Destination.PARENT_SOURCE);
	
	public final static VitalGraphArcElement CURRENTSRC_HYPEREDGE_PARENTDESTINATION = new VitalGraphArcElement(Source.CURRENT, Connector.HYPEREDGE, Destination.PARENT_DESTINATION);
	
	public final static VitalGraphArcElement CURRENTSRC_HYPEREDGE_PARENTCONNECTOR = new VitalGraphArcElement(Source.CURRENT, Connector.HYPEREDGE, Destination.PARENT_CONNECTOR);
	
	public final static VitalGraphArcElement PARENTCONNECTOR_HYPEREDGE_CURRENTDEST = new VitalGraphArcElement(Source.PARENT_CONNECTOR, Connector.HYPEREDGE, Destination.CURRENT);
	
	
	static {
		validArcs.addAll(Arrays.asList(
			PARENTSRC_HYPEREDGE_CURRENTDEST,
			PARENTDEST_HYPEREDGE_CURRENTDEST,
			CURRENTSRC_HYPEREDGE_PARENTSRC,
			CURRENTSRC_HYPEREDGE_PARENTDESTINATION,
			CURRENTSRC_HYPEREDGE_PARENTCONNECTOR,
			PARENTCONNECTOR_HYPEREDGE_CURRENTDEST
		));
	}
	
	private String firstSourceVar;
	private String firstConnectorVar;
	
	// bounds name to values
	Map<String, List<VitalGraphValue>> providesMap = new HashMap<String, List<VitalGraphValue>>();
	
	Map<String, Map<String, String>> providedName2URI2BoundVariable = new HashMap<String, Map<String, String>>();
	
	Set<String> referencedProvides = new HashSet<String>();
	
	Set<String> sortReferencedProvides = new HashSet<String>();
	
	Map<String, String> prefixesMap = new LinkedHashMap<String, String>();
	
	Map<String, Integer> prefixesHistogram = new HashMap<String, Integer>();
	
	Map<String, String> sortVariables = new HashMap<String, String>();
	
	// aggregation type
	private VitalSelectAggregationQuery aggSelectQuery = null;

	private VitalSelectQuery distinctSelectQuery;
	
	private VitalSelectQuery selectQuery;
	
	private boolean payloadsMode = false; 
	
	List<String> segmentURIs = new ArrayList<String>();
	
	private VitalGraphArcContainer topContainer = null;
	
	private QueryStats queryStats = null;
	
	public GraphQueryImplementation(VitalQuery _query, QueryStats queryStats) {
		
		if(_query instanceof VitalGraphQuery) {
			this.graphQuery = (VitalGraphQuery) _query;
		} 
		
		this.query = _query;
		
		this.queryStats = queryStats;
		
		if(_query instanceof VitalSelectAggregationQuery) {
			
			aggSelectQuery = (VitalSelectAggregationQuery)_query;
			topContainer = aggSelectQuery.getTopContainer();
			topFilterContainer = null;
			
		} else if(_query instanceof VitalSelectQuery) {
			
			topFilterContainer = null;
			
			VitalSelectQuery vsq = (VitalSelectQuery) _query;
			
			if(vsq.isDistinct()) {
				
				if( vsq.getPropertyURI() == null ) throw new RuntimeException("No property URI set in select distinct query");
				distinctSelectQuery = vsq;
				topContainer = distinctSelectQuery.getTopContainer();
				
			} else {
				
				selectQuery = vsq;
				
				topContainer = selectQuery.getTopContainer();
				
			}
		} else {
			
			topContainer = graphQuery.getTopContainer();
			
			payloadsMode = graphQuery.getPayloads();
			
			if(graphQuery.getTopContainer() == null) throw new NullPointerException("Top container mustn't be null");
			
			if(graphQuery.getTopContainer().isOptional()) throw new RuntimeException("OPTIONAL top arc container does not make any sense!");
			
		}
		
		// validate the query 
		
		if(graphQuery != null) {
			
			List<VitalSortProperty> sortProps = graphQuery.getSortProperties();
			
			if(sortProps != null) {
				
				for(VitalSortProperty sp : sortProps) {
					
					String providedName = sp.getProvidedName();
					
					if(providedName == null || providedName.isEmpty()) {
						
						throw new RuntimeException("Graph query's sort properties must have provided name set");
					}
					
					if(sortReferencedProvides.contains(providedName)) throw new RuntimeException("Each sort property must reference unique provided name, duplicated: " + providedName);
					
					sortReferencedProvides.add(providedName);
					
				}
				
			}
		}
		
		
		try {
			
			new TypeOneOfNoneOfOptimization(query.getSegments(), queryStats).processQuery(query);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		}
		
		collectProvideNames(topContainer, new HashSet<String>());
		
		// validate sort properties
		
		if(graphQuery != null) {
			
			List<VitalSortProperty> sortProps = graphQuery.getSortProperties();
			
			if(sortProps != null) {
		
				for(VitalSortProperty sp : sortProps) {
					
					String providedName = sp.getProvidedName();
					List<VitalGraphValue> l = providesMap.get(providedName);
					if(l == null) l = Collections.emptyList();
					boolean valid = false;
					for(VitalGraphValue v : l) {
						sp.setPropertyURI(v.getPropertyURI());
						valid = true;
						// if(v.getPropertyURI().equals(sp.getPropertyURI())) {
						// }
					}
					
					if(!valid) throw new RuntimeException("Vital sort property invalid, no provided propertyURI: " + sp.getPropertyURI() + " with name: " + sp.getProvidedName());
					
				}
				
			}
		}
		
		
		// preprocess (n)oneOf -> split such constraints into proper
		preprocessOneOf(topContainer);
		
		prefixesMap.put(RDF.getURI(), "rdf");
		prefixesMap.put(XSD.getURI(), "xsd");
		prefixesMap.put(VitalCoreOntology.NS, "vital-core");
		prefixesMap.put(VitalOntology.NS, "vital");
		
		// add for virtuoso
		prefixesMap.put("http://www.openlinksw.com/schemas/bif#", "bif");

		
		prefixesHistogram.put(RDF.getURI(), 1);
		prefixesHistogram.put(XSD.getURI(), 1);
		
		// force to always include this
		prefixesHistogram.put("http://www.openlinksw.com/schemas/bif#", 1);
		
		if(selectQuery != null) {
			
			setSelectSourceSymbol(topContainer);
			
		}
		
		// if(wrapper != null) {
		//	for(VitalSegment s : _query.getSegments()) {
		//		segmentURIs.add(s.getURI());
		//	}
		// }
	}
	
	void setSelectSourceSymbol( VitalGraphQueryContainer<?> c) {
		
		if(c == null) return;
		
		for( Object o : c ) {
			
			if( o instanceof VitalGraphQueryPropertyCriterion ) {
				
				VitalGraphQueryPropertyCriterion pc = (VitalGraphQueryPropertyCriterion) o;
			
				if(pc.getSymbol() == null) {
					pc.setSymbol(GraphElement.Source);
				}	
				
			} else if(o instanceof VitalGraphQueryContainer) {
				setSelectSourceSymbol((VitalGraphQueryContainer<?>) o);
			}
			
		}
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void preprocessOneOf(VitalGraphQueryContainer<?> container) {
		
		List newObjects = new ArrayList();
		
		for(Iterator<?> iterator = container.iterator(); iterator.hasNext(); ) {

			Object c = iterator.next();
			
			if(c instanceof VitalGraphQueryPropertyCriterion) {

				VitalGraphQueryPropertyCriterion crit = (VitalGraphQueryPropertyCriterion) c;
				
				
				/*
				
				//check if it's expand properties case
				if(crit.getExpandProperty()) {
					
					if(crit instanceof VitalGraphQueryTypeCriterion) {
						throw new RuntimeException("VitalGraphQueryTypeCriterion must not have expandProperty flag set!");
					}
					
					String propURI = crit.getPropertyURI();
					
					PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(propURI);
					
					if(pm == null) throw new RuntimeException("Property metadata not found: " + propURI);
				
					List<PropertyMetadata> subProperties = VitalSigns.get().getPropertiesRegistry().getSubProperties(pm, true);
					
					if(subProperties.size() > 1) {
						
						VitalGraphCriteriaContainer newContainer = new VitalGraphCriteriaContainer(QueryContainerType.or);
						
						for(PropertyMetadata p : subProperties) {
							
							VitalGraphQueryPropertyCriterion c2 = new VitalGraphQueryPropertyCriterion(p.getPattern().getURI());
							c2.setSymbol(crit.getSymbol());
							c2.setComparator(crit.getComparator());
							c2.setNegative(crit.isNegative());
							c2.setValue(crit.getValue());
							c2.setExpandProperty(false);
							c2.setExternalProperty(crit.getExternalProperty());
							
							newContainer.add(c2);
							
						}

						
						newObjects.add(newContainer);
						
						iterator.remove();
					}
					
				}
				*/
				
				
				//expand properties for EXIST, NOT_EXIST, CONTAINS, NOT_CONTAINS case, those test the existence of properties only
				Comparator x = crit.getComparator();
				
				if( crit.isExpandProperty() && ( x == Comparator.CONTAINS || x == Comparator.NOT_CONTAINS || x == Comparator.EXISTS || x == Comparator.NOT_EXISTS ) ){
					
					String propURI = crit.getPropertyURI();
					
					QueryContainerType type = ( 
							( !crit.isNegative() && (x == Comparator.NOT_CONTAINS || x == Comparator.NOT_EXISTS ) )
							|| (crit.isNegative() && ( x == Comparator.CONTAINS || x == Comparator.EXISTS)) )
							? QueryContainerType.and : QueryContainerType.or;
					
					
					if(propURI.equals(VitalGraphQueryPropertyCriterion.URI)) continue;
					
					PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(propURI);
					
					if(pm == null) throw new RuntimeException("Property metadata not found: " + propURI);
				
					List<PropertyMetadata> subProperties = VitalSigns.get().getPropertiesRegistry().getSubProperties(pm, true);

					if(subProperties.size() > 1) {
						
						VitalGraphCriteriaContainer newContainer = new VitalGraphCriteriaContainer(type);

						for(PropertyMetadata sub : subProperties) {
							
							VitalGraphQueryPropertyCriterion c2 = null;
							
							// if(c instanceof VitalGraphQueryTypeCriterion) {
							//
							// VitalGraphQueryTypeCriterion tc = (VitalGraphQueryTypeCriterion) c;
							//
							// c2 = new VitalGraphQueryTypeCriterion(tc.getType());
							//	
							// } else {
								
							c2 = new VitalGraphQueryPropertyCriterion(sub.getPattern().getURI());
								
							// }
							
							
							// if(VitalGraphQueryPropertyCriterion.URI.equals(crit.getPropertyURI())) {
							
							c2.setValue(crit.getValue());
							c2.setSymbol(crit.getSymbol());
							c2.setComparator(x);
							c2.setNegative(crit.isNegative());
							
							// } else {
							// c2
							// }
							
							newContainer.add(c2);
						}
						
						newObjects.add(newContainer);
						
						iterator.remove();
					}
				}
				
				if(crit.getComparator() == Comparator.ONE_OF || crit.getComparator() == Comparator.NONE_OF) {
					
					boolean none = crit.getComparator() == Comparator.NONE_OF;

					VitalGraphCriteriaContainer newContainer = new VitalGraphCriteriaContainer(none ? QueryContainerType.and : QueryContainerType.or);
					
					Object val = crit.getValue();
					
					if(!(val instanceof Collection)) throw new RuntimeException("Expected collection for comparator: " + crit.getComparator());
					
					for(Object v : (Collection)val) {
						
						VitalGraphQueryPropertyCriterion c2 = null;
						
						if(c instanceof VitalGraphQueryTypeCriterion) {
							
							VitalGraphQueryTypeCriterion tc = (VitalGraphQueryTypeCriterion) c;
							
							c2 = new VitalGraphQueryTypeCriterion(tc.getType());
							
						} else {
							
							c2 = new VitalGraphQueryPropertyCriterion(crit.getPropertyURI());			
						}
							
						// if(VitalGraphQueryPropertyCriterion.URI.equals(crit.getPropertyURI())) {
						
						c2.setValue(v);
						c2.setSymbol(crit.getSymbol());
						c2.setComparator(Comparator.EQ);
						c2.setNegative(none);
						
						// } else {
						// c2
						// }
						
						newContainer.add(c2);	
					}
					
					newObjects.add(newContainer);
					
					iterator.remove();
				}
				
				// special case for expand types criterion
				if(c instanceof VitalGraphQueryTypeCriterion) {
					
					VitalGraphQueryTypeCriterion tc = (VitalGraphQueryTypeCriterion) c;
					
					if(tc.isExpandTypes()) {
						
						Class<? extends GraphObject> gType = tc.getType();
						
						if(gType == null) throw new RuntimeException("No class set in type criterion" + tc);
						
						ClassMetadata cm = VitalSigns.get().getClassesRegistry().getClassMetadata(gType);
						
						if(cm == null) throw new RuntimeException("Class metadata not found for type: " + gType.getCanonicalName());
						
						List<ClassMetadata> subclasses = VitalSigns.get().getClassesRegistry().getSubclasses(cm, true);
						
						if(subclasses.size() > 1) {
							
							// only in this case add new container
							VitalGraphCriteriaContainer newContainer = new VitalGraphCriteriaContainer(QueryContainerType.or);
							
							for(ClassMetadata m : subclasses) {
																
								VitalGraphQueryTypeCriterion nt = new VitalGraphQueryTypeCriterion(GraphElement.Destination, m.getClazz());

								newContainer.add(nt);			
							}
		
							newObjects.add(newContainer);
							
							iterator.remove();
						}
					}
				}
				
				continue;
			}
			
			if(c instanceof VitalGraphQueryContainer) {
				preprocessOneOf((VitalGraphQueryContainer)c);
			}
		}
		
		container.addAll(newObjects);
	}

	@SuppressWarnings("unchecked")
	private void collectProvideNames(VitalGraphQueryContainer<VitalGraphQueryContainer<?>> container, Set<String> parentContext) {

		Set<String> thisContext = new HashSet<String>();
		
		if(container instanceof VitalGraphArcContainer) {

			VitalGraphArcContainer arc = (VitalGraphArcContainer) container;
			
			for( Entry<String, VitalGraphValue> entry : arc.getProvidesMap().entrySet()) {
				
				String name = entry.getKey();
				
				// if(providesMap.containsKey(name)) {
				// throw new RuntimeException("Name provided more than once in the query: " + name);
				// }
				
				
				List<VitalGraphValue> v = providesMap.get(name);
				if(v == null) {
					v = new ArrayList<VitalGraphValue>();
					providesMap.put(name, v);
				}
				
				v.add( entry.getValue() ) ;
				
				thisContext.add(name);
			}
		}
		
		for(VitalGraphQueryContainer<?> c: container) {
			
			if(c instanceof VitalGraphArcContainer || c instanceof VitalGraphBooleanContainer) {
				collectProvideNames((VitalGraphQueryContainer<VitalGraphQueryContainer<?>>) c, thisContext);
			}	
		}
		
		if(container instanceof VitalGraphArcContainer) {

			VitalGraphArcContainer arc = (VitalGraphArcContainer) container;
			
			for(VitalGraphValueCriterion crit : arc.getValueCriteria()) {
				
				String n1 = crit.getName1();
				String n2 = crit.getName2();
				
				if(n1 != null ) {
					
					if( !thisContext.contains(n1) ) {
						throw new RuntimeException("Value with name not found in container: " + n1) ;
					}
					
					referencedProvides.add(n1);
				}
				
				if(n2 != null) {
					
					if( !thisContext.contains(n2) ) {
						throw new RuntimeException("Value with name not found in container: " + n2);
					}
					
					referencedProvides.add(n2);
				}
			}
		}
		
		parentContext.addAll(thisContext);
	}

	static Map<Comparator, List<Class<?>>> comparator2Classes = new HashMap<Comparator, List<Class<?>>>();
	
	static {
		// all
		
		comparator2Classes.put(Comparator.EQ, new ArrayList<Class<?>>());
		comparator2Classes.put(Comparator.NE, new ArrayList<Class<?>>());
		comparator2Classes.put(Comparator.EQ_CASE_INSENSITIVE, new ArrayList<Class<?>>());
		comparator2Classes.put(Comparator.CONTAINS_CASE_SENSITIVE, new ArrayList<Class<?>>(Arrays.asList(String.class)));
		comparator2Classes.put(Comparator.CONTAINS_CASE_INSENSITIVE, new ArrayList<Class<?>>(Arrays.asList(String.class)));
		List<Class<?>> numericClasses = new ArrayList<Class<?>>(Arrays.asList(Integer.class, Long.class, Float.class, Double.class, Date.class));
		comparator2Classes.put(Comparator.GE, numericClasses);
		comparator2Classes.put(Comparator.GT, numericClasses);
		comparator2Classes.put(Comparator.LE, numericClasses);
		comparator2Classes.put(Comparator.LT, numericClasses);
		comparator2Classes.put(Comparator.REGEXP, new ArrayList<Class<?>>(Arrays.asList(String.class)));
		comparator2Classes.put(Comparator.REGEXP_CASE_SENSITIVE, new ArrayList<Class<?>>(Arrays.asList(String.class)));
		comparator2Classes.put(Comparator.CONTAINS, new ArrayList<Class<?>>(Arrays.asList(Boolean.class, Truth.class, Integer.class, Long.class, Float.class, Double.class, Date.class, String.class)));
		comparator2Classes.put(Comparator.NOT_CONTAINS, new ArrayList<Class<?>>(Arrays.asList(Boolean.class, Truth.class, Integer.class, Long.class, Float.class, Double.class, Date.class, String.class)));
	}
	
	public String generateSparqlQuery() {
		
		// start where
		
		builder.append(" WHERE {\n");
		processContainer(null, topContainer, topFilterContainer,  ind, true);
		
		
		if(topFilterContainer != null) {
			processTopFilterConstainer();
		}
		
		
		// close where
		builder.append("}\n");
		
		
		// now 
		// builder.insert(0, obj)

		String bindindsString = null;
		
		if(overriddenBindingsString != null) {
			
			bindindsString = overriddenBindingsString;
			
			//not a graph query ? 
			bindingNames = null;
			
		} else {
			
			bindindsString = "DISTINCT";
			
			for(String binding : bindings) {
				
				if(bindindsString.length() > 0) bindindsString += " ";
				
				bindindsString += binding;
				
			}
			
		}
		
		bindindsString += "\n";
		
		String contextsString = "";
		
		if(segmentURIs != null) {
			for(String ctx : segmentURIs) {
				contextsString += "FROM <" + ctx + ">\n";
			}
		}
		
		if(aggSelectQuery != null) {
			
			// don't do anything
			
		} else if(distinctSelectQuery != null) {
			
			if(distinctSelectQuery.getDistinctSort() != null) {
				String order = null;
				if( VitalSelectQuery.asc.equals(distinctSelectQuery.getDistinctSort()) ) {
					order = distinctSelectQuery.getDistinctLast() ? "DESC" : "ASC";
				} else {
					order = distinctSelectQuery.getDistinctLast() ? "ASC" : "DESC";
				}
				builder.append("\nORDER BY " + order + "(" + distinctValueVariable + ")");
				
				if(distinctSelectQuery.getDistinctFirst() || distinctSelectQuery.getDistinctLast()) {
					
					builder.append("\nOFFSET 0 LIMIT 1");
					
				} else {
					
					if(distinctSelectQuery.getLimit() > 0 && distinctSelectQuery.getOffset() >= 0 ) {
						
						builder.append("\nOFFSET " + distinctSelectQuery.getOffset() + " LIMIT " + distinctSelectQuery.getLimit());		
					}
				}
			}

		} else if(selectQuery != null) {
			
			if(selectQuery.isProjectionOnly()) {
				
				//ignore pagination and sort
				
			} else {
				
				// sort
				List<VitalSortProperty> sortProperties = selectQuery.getSortProperties();
				
				builder.append("\nORDER BY " );
				
				if(sortProperties.size() == 0) {
					
					builder.append( firstSourceVar + " ");
					
				} else {
					
					for(VitalSortProperty vsp : sortProperties) {
						
						String f = vsp.isReverse() ? "DESC" : "ASC";
						
						String sortVar = sortVariables.get(vsp.getPropertyURI());
						
						if(sortVar == null) throw new RuntimeException("Sort variable not found for property: " + vsp.getPropertyURI());
						
						builder.append(f + "(" + sortVar + ") ");
						
					}
					
				}
				
				if( selectQuery.getLimit() > 0 || selectQuery.getOffset() > 0) {
					
					if(selectQuery.getOffset() < 0) throw new RuntimeException("Offset must be >= 0 when query limit used.");
					
					builder.append("\nOFFSET ").append(selectQuery.getOffset());
					if(selectQuery.getLimit() > 0) {
						builder.append(" LIMIT ").append(selectQuery.getLimit());
					}
				}
			}
			
		} else {
			
			List<VitalSortProperty> sortProperties = graphQuery.getSortProperties();
			
			if(sortProperties == null) sortProperties = Collections.emptyList();
			
			if( graphQuery.getLimit() > 0 || graphQuery.getOffset() > 0) {
				
				if(graphQuery.getOffset() < 0) throw new RuntimeException("Offset must be >= 0 when query limit used.");
				
				if(sortProperties.size() == 0) {
					
					// order by first 2 variables ? 
					builder.append("\nORDER BY " + firstSourceVar + " ");
					if(firstConnectorVar != null) {
						builder.append(firstConnectorVar);
					}
					
				}
			}
			
			if(sortProperties.size() > 0) {
				
				builder.append("\nORDER BY " );
				
				for(VitalSortProperty vsp : sortProperties) {
					
					String f = vsp.isReverse() ? "DESC" : "ASC";
					
					Map<String, String> map = providedName2URI2BoundVariable.get(vsp.getProvidedName());
					if(map == null) throw new RuntimeException("No sort provided variable info, provided name: " + vsp.getProvidedName());
					String boundVar = map.get(vsp.getPropertyURI());
					if(boundVar == null) throw new RuntimeException("No bound variable found for provided name: " + vsp.getProvidedName() + ", proertyURI: " + vsp.getPropertyURI());
					
					builder.append(f + "(" + boundVar + ") ");	
				}
			}
			
			if( graphQuery.getLimit() > 0 || graphQuery.getOffset() > 0) {
				
				builder.append("\nOFFSET ").append(graphQuery.getOffset());
				
				if(graphQuery.getLimit() > 0) {
					builder.append(" LIMIT ").append(graphQuery.getLimit());
				}
			}
		}
		
		String prefixes = "";
		
		for(Entry<String, String> p : prefixesMap.entrySet()) {
			
			if(prefixesHistogram.containsKey(p.getKey())) {
				
				prefixes += ("PREFIX " + p.getValue() + ": <" + p.getKey() + ">\n");
			}
		}
		
		return prefixes + "\nSELECT " + bindindsString + contextsString + builder.toString();
	}

	private void processTopFilterConstainer() {

		cleanupContainer(topFilterContainer);
		
		if(topFilterContainer.containers.size() == 0 && topFilterContainer.filters.size() == 0) return;
		
		builder.append("FILTER (\n");
		
		processFilterConatiner("  ", topFilterContainer);
		
		builder.append(")\n");
		
	}

	private void cleanupContainer(FilterContainer c) {

		for(FilterContainer child : new ArrayList<FilterContainer>(c.containers)) {
			
			cleanupContainer(child);	
		}
		
		if(c.filters.size() == 0 && c.containers.size() == 0) {
			
			if(c.parent != null) {
				
				c.parent.containers.remove(c);
				
				c.parent = null;
				
			}
		}
	}

	private void processFilterConatiner(String indent, FilterContainer filterContainer) {

		boolean first = true;
		
		for(FilterContainer c : filterContainer.containers ) {

			// skip empty containers
			if(first) {
				first = false;
			} else {
				
				String link = filterContainer instanceof FilterAND ? "&&" : "||";
				builder.append(indent + link + "\n");
			}
			
			builder.append(indent + "(\n");
			
			processFilterConatiner(indent + "  ", c);
			
			builder.append(indent + ")\n");
		}
		
		for(String l : filterContainer.filters) {
			
			if(first) {
				first = false;
			} else {
				String link = filterContainer instanceof FilterAND ? "&&" : "||";
				builder.append(indent + link + "\n");
			}
			
			builder.append(indent + l + "\n");	
		}	
	}

	private void processContainer(ContainerContext parentContainerContext, VitalGraphQueryContainer<?> container, FilterContainer filterContainer, String indent, boolean firstChild) {

		VitalGraphArcContainer thisContainer =  container instanceof VitalGraphArcContainer ? (VitalGraphArcContainer) container : null;
				
		if( thisContainer != null && thisContainer.isOptional() )  {
			
			//optional graph pattern must keep their filters inside
			
			filterContainer = null;
		}
		
		
		// TODO
		// mch
		// test forcing filters into the main body?
		
		filterContainer = null;
		
		// this seems to work for the initial test case
		// ideally this should just be for the contains case 
		// to push bif:contains into the section binding the input value
		// like:
		/*
		 ?slot p0:vital__hasTextSlotValue ?value4 . 
        FILTER (
          (
            bif:contains(?value4,"'Happy'")   
		 */
		
		// i think there was some reason before to push the filters to the end
		// so will need to test
		
		
		
		// look for arcs
		
		if(!firstChild && parentContainerContext != null && parentContainerContext.type == QueryContainerType.or) {
			builder.append(indent + "UNION {\n");

			// always nest OPTIONAL clauses
		} else if( thisContainer != null && thisContainer.isOptional() /* && ( parentContainerContext == null || !parentContainerContext.optionalContext )*/ ) {
			builder.append(indent + "OPTIONAL {\n");
		} else {
			builder.append(indent + "{\n");
		}
		
		if(container instanceof VitalGraphBooleanContainer) {
			
			VitalGraphBooleanContainer bc = (VitalGraphBooleanContainer) container;

			QueryContainerType type = bc.getType();
			
			boolean first = true;
			
			String subIndent = indent + ind; 
				
			QueryContainerType originalType = parentContainerContext.type;
			
			// just change the type 
			parentContainerContext.type = type;
			
			for(VitalGraphQueryContainer<?> c : bc) {
							
				if(c instanceof VitalGraphCriteriaContainer) {
					throw new RuntimeException("Boolean containers may only contain arc or boolean containers, not criteria containers");
				}
				
				FilterContainer newFilterContainer = null;
				
				if(filterContainer != null) {
					
					newFilterContainer = c.getType() == QueryContainerType.and ? new FilterAND(filterContainer) : new FilterOR(filterContainer);
				}
				
				processContainer(parentContainerContext, c, newFilterContainer, subIndent, first);
					
				first = false;				
			}
			
			// close block
			builder.append("\n" + indent + "}\n");
			
			// restore old type
			parentContainerContext.type = originalType;
			
			return;
		}
		
		counter++;
		
		VitalGraphArcElement arc = thisContainer.getArc();
		
		if(arc == null) throw new RuntimeException("No arc set in a container!");
		
		boolean valid = false;
		
		for(VitalGraphArcElement validArc : validArcs) {
			
			if(areEqual(validArc, arc)) {
				valid = true;
				break;
			}
			
		}
		
		if(!valid) throw new RuntimeException("Unsupported arc pattern: " + arc);
		
		if(parentContainerContext == null) {
			
			//verify arc
			if( arc.source == Source.PARENT_SOURCE || arc.source == Source.PARENT_CONNECTOR || arc.source == Source.PARENT_DESTINATION ) {
				throw new RuntimeException("Cannot map source to parent element in the top arc!");
			}
			
			if( arc.destination == Destination.PARENT_SOURCE || arc.destination == Destination.PARENT_CONNECTOR || arc.destination == Destination.PARENT_DESTINATION) {
				throw new RuntimeException("Cannot map source to parent element in the top arc!");
			}
			
		}
		
		String sourceVar = null;
		
		boolean sourceMapped = false;
		
		if(arc.source == Source.PARENT_SOURCE) {
			
			sourceVar = parentContainerContext.sourceVar;
			if(sourceVar == null) throw new RuntimeException("Parent arc source unbound.");
			sourceMapped = true;
			
		} else if(arc.source == Source.PARENT_CONNECTOR) {
			
			sourceVar = parentContainerContext.connectorVar;
			if(sourceVar == null) throw new RuntimeException("Parent arc connector unbound.");
			sourceMapped = true;
			
		} else if(arc.source == Source.PARENT_DESTINATION) {
			
			sourceVar = parentContainerContext.destinationVar;
			if(sourceVar == null) throw new RuntimeException("Parent arc destination unbound.");
			sourceMapped = true;
			
		} else if(arc.source == Source.CURRENT) {
			
			if( thisContainer.getSourceBind() != null ) {
				sourceVar = ( "?" + thisContainer.getSourceBind() );
			} else {
				sourceVar = ( "?source" + counter );
			}
			
			if(thisContainer.getCapture() == Capture.BOTH || thisContainer.getCapture() == Capture.TARGET || thisContainer.getCapture() == Capture.SOURCE) {
				bindings.add(sourceVar);
				bindingNames.add(sourceVar.substring(1));
			}
		}
			
		if(firstSourceVar == null) {
			
			firstSourceVar = sourceVar;
		}
	
		String connectorVar = null;
		
		if( arc.connector != Connector.EMPTY) {
			
			if(thisContainer.getConnectorBind() != null) {
				connectorVar = "?" + thisContainer.getConnectorBind();
			} else {
				connectorVar = "?connector" + counter;
			}
		}
		
		if(connectorVar != null) {
			
			if(firstConnectorVar == null) {
				firstConnectorVar = connectorVar;
			}
			
			if(thisContainer.getCapture() == Capture.BOTH || thisContainer.getCapture() == Capture.CONNECTOR) {
				bindings.add(connectorVar);
				bindingNames.add(connectorVar.substring(1));
			}
		}
		
		
		String destinationVar = null;
		
		boolean destinationMapped = false;
		
		if(arc.destination == Destination.PARENT_SOURCE) {
			
			destinationVar = parentContainerContext.sourceVar;
			if(destinationVar == null) throw new RuntimeException("Parent arc destination unbound.");
			destinationMapped = true;
			
		} else if(arc.destination == Destination.PARENT_CONNECTOR) {
			
			destinationVar = parentContainerContext.connectorVar;
			if(destinationVar == null) throw new RuntimeException("Parent arc connector unbound.");
			destinationMapped = true;
			
		} else if(arc.destination == Destination.PARENT_DESTINATION) {
			
			destinationVar = parentContainerContext.destinationVar;
			if(destinationVar == null) throw new RuntimeException("Parent arc destination unbound.");
			destinationMapped = true;
			
		} else if(arc.destination == Destination.CURRENT) {
			
			if( thisContainer.getTargetBind() != null) {
				destinationVar = ( "?" + thisContainer.getTargetBind() );
				
			} else {
				destinationVar = ( "?target" + counter );
			}
			
			if(thisContainer.getCapture() == Capture.BOTH || thisContainer.getCapture() == Capture.TARGET) {
				bindings.add(destinationVar);
				bindingNames.add(destinationVar.substring(1));
			}
			
		}
		
		
		// String node1Var = arc.node1 != Node1.EMPTY ? ( "?s" + counter ) : null;
		// String node2Var = arc.node2 != Node2.EMPTY ? ( "?d" + counter ) : null;
		// if(node1Var != null) bindings.add(node1Var);
		// if(node2Var != null) bindings.add(node2Var);
		

		ContainerContext thisContainerContext = new ContainerContext();
		thisContainerContext.container = thisContainer;
		thisContainerContext.sourceVar = sourceVar;
		thisContainerContext.connectorVar = connectorVar;
		thisContainerContext.destinationVar = destinationVar;
		thisContainerContext.arc = arc;

		//optional propagates down to children
		thisContainerContext.optionalContext = thisContainer.isOptional() || (parentContainerContext != null && parentContainerContext.optionalContext); 
		thisContainerContext.type = thisContainer.getType();
		
		if( thisContainerContext.optionalContext &&  thisContainer.getCapture() != Capture.BOTH ) {
			throw new RuntimeException("Capture BOTH flag must be always set in optional arcs and their children!");
		}
		
		if(parentContainerContext != null && parentContainerContext.type == QueryContainerType.or && thisContainer.isOptional()) {
			throw new RuntimeException("Cannot use optional arc container in an OR parent arc container!");
		}
		
		
		// if(query instanceof VitalSelectQuery) {
		//	
		//just insert the artificial line
		// builder.append(indent + ind + sourceVar + " ?px ?ox . \n");
		//	
		// }
		
		
		// String mapVariable = arc.node1 == Node1.PARENT ? node1Var : ( arc.node2 == Node2.PARENT ? node2Var : null); 
		
		// check regular containers count, if it's one then just place the 
		
		List<VitalGraphQueryContainer<?>> subArcsOrBooleans = new ArrayList<VitalGraphQueryContainer<?>>();
		
		List<VitalGraphCriteriaContainer> subCrits = new ArrayList<VitalGraphCriteriaContainer>();
		
		for(VitalGraphQueryContainer<?> element : thisContainer ) {
			
			if(element instanceof VitalGraphArcContainer || element instanceof VitalGraphBooleanContainer) {
				
				subArcsOrBooleans.add(element);
				
			} else if(element instanceof VitalGraphCriteriaContainer) {
				
				subCrits.add((VitalGraphCriteriaContainer) element);
				
			}
			
		}
		
		
		// new way
		if(subCrits.size() > 0) {
			
			//if more than 1 then create an artificial boolean or, otherwise just pass the single crits container
			
			VitalGraphCriteriaContainer criteria = new VitalGraphCriteriaContainer(QueryContainerType.and);
			
			for(VitalGraphCriteriaContainer c : subCrits) {
				criteria.add(c);
			}
			
			FilterContainer newFilterContainer = null;
			
			if(filterContainer != null) {
				newFilterContainer = new FilterAND(filterContainer);
			}
			
			
			// if(subCrits.size() > 1) {
			// criteria = new VitalGraphCriteriaContainer(QueryContainerType.and);
			// for(VitalGraphCriteriaContainer c : subCrits) {
			// criteria.add(c);
			// }
			// } else {
			// criteria = subCrits.get(0);
			// }
			
			processTopCriteriaContainersV2(thisContainerContext, criteria, newFilterContainer, indent + ind);
			
		}
		
		/* XXX old way
		
		VitalGraphCriteriaContainer prev = null;
		
		for(VitalGraphCriteriaContainer subCrit : subCrits) {
			
			if(subCrits.size() > 1) {
				
				//open block
				
				if(prev != null) {
					
					builder.append(indent + " UNION {\n");
					
				} else {
					
					builder.append(indent + "{\n");
					
				}
				
			}

			processCriteriaContainer(thisContainerContext, subCrit, indent + ind);
			
			if(subCrits.size() > 1) {
				
				builder.append(indent + "}\n");
				
			}
			
			prev = subCrit;
			
		}
		*/
		
		
		boolean first = true;
		
		for(VitalGraphQueryContainer<?> subArc : subArcsOrBooleans) {

			FilterContainer newFilterContainer = null;
			
			if(filterContainer != null) {
				newFilterContainer = subArc.getType() == QueryContainerType.and ? new FilterAND(filterContainer) : new FilterOR(filterContainer);
			}
			
			processContainer(thisContainerContext, subArc, newFilterContainer, indent + ind, first);
			
			first = false;	
		}
		
		// append filters
		
		if(sourceMapped || destinationMapped) {

			if ( arc.connector == Connector.EMPTY ) throw new RuntimeException("Cannot use empty connector!");
			
			Property srcProp = arc.connector == Connector.EDGE ? VitalCoreOntology.hasEdgeSource : VitalCoreOntology.hasHyperEdgeSource;
			
			Property destProp = arc.connector == Connector.EDGE ? VitalCoreOntology.hasEdgeDestination : VitalCoreOntology.hasHyperEdgeDestination;
			
			// append edge variable
			builder.append(indent + ind + connectorVar + " " + predicate(srcProp.getURI()) + " " + sourceVar + " . \n");
			builder.append(indent + ind + connectorVar + " " + predicate(destProp.getURI()) + " " + destinationVar + " . \n");
			
			// skipping filters - very inefficient
				
		}
		
		for(Entry<String, VitalGraphValue> provideEntry : thisContainer.getProvidesMap().entrySet()) {
			
			String provideName = provideEntry.getKey();
			
			if(!referencedProvides.contains(provideName) && !sortReferencedProvides.contains(provideName)) {
				
				log.warn("skipping non-referenced provided value: " + provideName);
				
				continue;
				
			}
			
			String src = null;
			
			VitalGraphValue provide = provideEntry.getValue();
			
			switch (provide.getSymbol() ) { 
				case Source: 
					src = thisContainerContext.sourceVar;
					break;
				case Connector: 
					src = thisContainerContext.connectorVar;
					break;
				case Destination:
					src = thisContainerContext.destinationVar;
					break;
				default: 
					break;
			}
			
			if(src == null) {
				throw new RuntimeException("provide's symbol variable not found: " + provideName + " " + provide.getSymbol() + " " + provide.getPropertyURI());
			}
			
			String boundVar = null;
			
			if(VitalGraphValue.URI.equals(provide.getPropertyURI())) {
				
				boundVar = src;
				
			} else {
				
				boundVar = "?bound" + boundCounter++;
				
				//optional
				if(!referencedProvides.contains(provideName) && sortReferencedProvides.contains(provideName)) {
					
					builder.append(indent + ind + "OPTIONAL { " + src + " " + predicate(provide.getPropertyURI()) + " " + boundVar + " } . \n");
					
				} else {
					
					builder.append(indent + ind + src + " " + predicate(provide.getPropertyURI()) + " " + boundVar + " . \n");
					
				}
			}
			
			builder.append(indent + ind + "BIND ( " + boundVar + " as ?" + provideName + " ) . \n");
		
			Map<String, String> map = providedName2URI2BoundVariable.get(provideName);
			if(map == null) {
				map = new HashMap<String, String>();
				providedName2URI2BoundVariable.put(provideName, map);
			}
			
			if(map.containsKey(provide.getPropertyURI())) throw new RuntimeException("Provided name " + provideName + " with property uri " + provide.getPropertyURI() + " already bound to a variable");
			
			map.put(provide.getPropertyURI(), boundVar);
			
		}
		
		//check provides
		
		for( VitalGraphValueCriterion criterion : thisContainer.getValueCriteria() ) {
			
			// VitalGraphProvides p1 = providesMap.get( criterion.getName1() );
			//
			// VitalGraphProvides p2 = providesMap.get( criterion.getName2() );
			
			VitalGraphValueCriterion.Comparator comparator = criterion.getComparator();
			
			String op = null;
			
			switch(comparator) {
				case EQ: 
					op = "=";
					break;
				case GE:
					break;
				case LE:
					op = "<=";
					break;
				case LT:
					op = "<";
					break;
				case NE:
					op = "!=";
					break;
				default:
					break;
			}
			
			// if( ( c == Comparator.GE && !pc.isNegative() ) || ( c == Comparator.LT && pc.isNegative() ) ) {
			//	
			// operator = ">=";
			//
			// } else if( ( c == Comparator.GT && !pc.isNegative() ) || ( c == Comparator.LE && pc.isNegative() ) ) {
			//
			// operator = ">";
			//	
			// } else if( ( c == Comparator.LE && !pc.isNegative() ) || ( c == Comparator.GT && pc.isNegative() ) ) {
			//	
			// operator = "<=";
			//
			// } else if( ( c == Comparator.LT && !pc.isNegative() ) || ( c == Comparator.GE && pc.isNegative() ) ) {
			//	
			// operator = "<";
			//
			
			String v1 = null;
			
			if(criterion.getName1() != null) {
				
				v1 = "?" + criterion.getName1();
				
			} else {
				
				GraphElement symbol = criterion.getValue1().getSymbol();

				switch(symbol) {
					case Source:
						v1 = thisContainerContext.sourceVar;
						break;
					case Connector: 
						v1 = thisContainerContext.connectorVar;
						break;
					case Destination:
						v1 = thisContainerContext.destinationVar;
						break;
					default:
						break;
				}
				
				if(v1 == null) throw new RuntimeException("Value not bound - missing arc element: " + symbol);

				if(VitalGraphValue.URI.equals(criterion.getValue1().getPropertyURI())) {
					
				} else {
					
					String newVar = "?pr" + ++providesCounter; 
							
					// generate some temporary variable
					
					builder.append(indent + ind + v1 + "<" + criterion.getValue1().getPropertyURI() + "> " + newVar + " . \n");
					
					v1 = newVar;
				}
			}

			String v2 = null;
			
			if(criterion.getName2() != null ) {
				
				v2 = "?" + criterion.getName2();
				
			} else {
				
				GraphElement symbol = criterion.getValue2().getSymbol();

				switch(symbol) {
					case Source:
						v2 = thisContainerContext.sourceVar;
						break;
					case Connector: 
						v2 = thisContainerContext.connectorVar;
						break;
					case Destination:
						v2 = thisContainerContext.destinationVar;
						break;
					default:
						break;
				}
				
				if(v2 == null) throw new RuntimeException("Value not bound - missing arc element: " + symbol);

				if(VitalGraphValue.URI.equals(criterion.getValue2().getPropertyURI())) {
					
				} else {
					
					String newVar = "?pr" + ++providesCounter; 
							
					// generate some temporary variable 
					builder.append(indent + ind + v2 + "<" + criterion.getValue1().getPropertyURI() + "> " + newVar + " . \n");
					
					v2 = newVar;
					
				}
			}
			
			if(filterContainer != null) {
				filterContainer.filters.add("( " + v1 + " " + op + " " + v2 + " )");
				
			} else {
				builder.append(indent + ind + "FILTER ( " + v1 + " " + op + " " + v2 + " ) . \n");
				
			}
		}
		
		// close block
		builder.append("\n" + indent + "}\n");
	}

	int prefixesCounter = 0;

	private String distinctValueVariable;
	
	// private String aggregationValueVariable;
	
	private String predicate(String uri) {
		
		// replace types predicate with RDF type
		if(VitalCoreOntology.types.getURI().equals(uri)) {
			
			uri = RDF.type.getURI();
			
		} else {
			
			if( VitalSigns.get().getPropertiesRegistry().getProperty(uri) != null ) {
				
				uri = RDFVitalPropertyFilter.vitalPropertyPrefixInjector(uri);
			}
		}
		
		return uriVal(uri);
	}
	
	private String uriVal(String uri) {
		
		int ind = uri.lastIndexOf('#');
		
		if(ind > 0 ) {
			
			String ns = uri.substring(0, ind + 1);
			
			String pref = prefixesMap.get(ns);
			
			if(pref == null) {
				pref = "p" + prefixesCounter++;
				prefixesMap.put(ns, pref);
			}

			Integer integer = prefixesHistogram.get(ns);
			
			if(integer == null) integer = 1; else integer = integer+1;
			
			prefixesHistogram.put(ns, integer);
			
			uri = pref + ":" + uri.substring(ind + 1);
			
		} else {
			
			uri = "<" + uri + ">";
		}
		
		return uri;
	}

	static class ContainerContext {
		
		VitalGraphArcContainer container;
		
		VitalGraphArcElement arc;
		
		String sourceVar = null;
	
		String connectorVar = null;
		
		String destinationVar = null;

		boolean optionalContext = false;
		
		QueryContainerType type;
		
	}
	
	Double aggResult = null;
	
	Integer totalResults = null; 
	
	@SuppressWarnings("deprecation")
	public ResultList handle() throws Exception {
		
		long totalTime = System.currentTimeMillis();
		
		String sparqlString = generateSparqlQuery();
		
		final ResultList rl = new ResultList();
		
		Integer limit = null;
		Integer offset = null;
		
		if(graphQuery != null) {
			limit = graphQuery.getLimit();
			offset = graphQuery.getOffset();
			rl.setBindingNames(bindingNames);
		} else if(selectQuery != null) {
			if(selectQuery.isProjectionOnly()) {
				
			} else {
				limit = selectQuery.getLimit();
				offset = selectQuery.getOffset();
			}
		} else if(aggSelectQuery != null) {
			limit = aggSelectQuery.getLimit();
			offset = aggSelectQuery.getOffset();
		} else if(distinctSelectQuery != null) {
			limit = distinctSelectQuery.getLimit();
			offset = distinctSelectQuery.getOffset();
		}
		
		if(limit != null) rl.setLimit(limit);
		
		if(offset != null) rl.setOffset(offset);
		
		if(query.getReturnSparqlString()) {
			rl.setStatus(VitalStatus.withOKMessage(sparqlString));
			return rl;
		}
		
		final Map<String, GraphObject> payloads = payloadsMode ? new HashMap<String, GraphObject>() : null;
		
		final List<String> selectURIs = selectQuery != null ? new ArrayList<String>() : null;
		
		SPARQLResultsJSONParser parser = new SPARQLResultsJSONParser();
		
		parser.setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
			
			@Override
			public void handleSolution(BindingSet next)
					throws TupleQueryResultHandlerException {


				// List<URIProperty> uris = new ArrayList<URIProperty>();
								
				
				GraphMatch match = new GraphMatch();
				
				match.setURI(URIGenerator.generateURI(null, GraphMatch.class, true));

				int c = 0;
				
				for(Binding b : next) {
					
					if(distinctSelectQuery != null) {

						// collect all distinct values - select 
						Value value = b.getValue();
						
						Object valueToJavaValue = RDFSerialization.valueToJavaValue(value);
						
						match.setProperty("value", valueToJavaValue);
						
						c++;
						
					} else if(aggSelectQuery != null) {
						
						
						// if(aggregationValueVariable.equals(b.getName();
						Literal lit = ((Literal)b.getValue());
						aggResult = lit.doubleValue();
						
						
					} else if(selectQuery != null) {
						
						if(selectQuery.isProjectionOnly()) {
							
							Literal lit = ((Literal) b.getValue());
							
							log.info("Projection Results: " + lit.intValue());
							
							totalResults = lit.intValue();
							
						} else {
							
							String uri = b.getValue().stringValue();
							
							if(selectURIs.indexOf(uri) < 0) {
								selectURIs.add(uri);
							}
						}
						
					} else {
						if( b.getValue() instanceof URI ) {
							String uri = b.getValue().stringValue();
							if(payloads != null) payloads.put(uri, null);
							URIProperty u = new URIProperty(uri);
							// uris.add(u);
							match.setProperty(b.getName(), u);
							c++;
						}								
					}
				}
				
				// if(uris.size() < 1) continue;
				if(c < 1) return;
				
				// whileIterator<Binding> iterator = next.iterator();
				// match.setProperty("graphURIs", uris);
				
				rl.getResults().add(new ResultElement(match, 1D));
			}
		});
		
		long start = System.currentTimeMillis();
		
		if(queryStats != null) {
			long time = queryStats.addDatabaseTimeFrom(start);
			
			if(queryStats.getQueriesTimes() != null) queryStats.getQueriesTimes().add(new QueryTime(query.debugString(), sparqlString, time));
		}
		
		if(aggSelectQuery != null) {
			
			if(aggResult == null) throw new RuntimeException("No aggResult found in response!");
			
			AggregationResult res = new AggregationResult();
			res.setURI(URIGenerator.generateURI(null, res.getClass(), true));
			res.setProperty("aggregationType", aggSelectQuery.getAggregationType().name());
			res.setProperty("value", aggResult);
			
			rl.getResults().add(new ResultElement(res, 1D));	
		}
		
		if(selectQuery != null) {

			if(selectQuery.isProjectionOnly()) {
				
				
				if(totalResults == null) throw new RuntimeException("No totalResults found in response!");
				
				rl.setTotalResults(totalResults);
				
			} else {
				
				Map<String, GraphObject> m = new HashMap<String, GraphObject>();
				
				if(selectURIs.size() > 0) { 
					
					// List<GraphObject> objects = wrapper._getBatch(segmentURIs.toArray(new String[segmentURIs.size()]), selectURIs, queryStats);
					
					List<GraphObject> objects = new ArrayList<GraphObject>();

					for(GraphObject g : objects) {
						m.put(g.getURI(), g);
					}
				}
	
				for(String uri : selectURIs) {
					
					GraphObject g = m.get(uri);
					
					if(g != null) {
						
						rl.getResults().add(new ResultElement(g, 1D));
						
					}
				}
			}
			
		} else {
			
			if(payloads != null && payloads.size() > 0) {
				
				// List<GraphObject> objects = wrapper._getBatch(segmentURIs.toArray(new String[segmentURIs.size()]), payloads.keySet(), queryStats);
				
				List<GraphObject> objects = new ArrayList<GraphObject>();
				
				for(GraphObject g : objects) {
					payloads.put(g.getURI(), g);
				}
				
				for(GraphObject g : rl) {
					
					GraphMatch gm = (GraphMatch) g;
					
					for(IProperty p : new ArrayList<IProperty>(gm.getPropertiesMap().values())) {
						if(p instanceof URIProperty) {
							GraphObject x = payloads.remove( ((URIProperty)p).get() );
							if(x != null) {
								gm.setProperty(x.getURI(), x.toCompactString());
							}
						}
					}
				}
				
				// if expand
				
			}
		}	
		
		if(rl.getTotalResults() == null) {
			rl.setTotalResults(rl.getResults().size());
		}
		
		rl.setQueryStats(queryStats);
		if(queryStats != null) {
			queryStats.setQueryTimeMS(System.currentTimeMillis() - totalTime);
		}
		
		return rl;
	}
	
	public boolean areEqual(VitalGraphArcElement _this, VitalGraphArcElement that) {
		return
			_this.source == that.source &&
			_this.connector == that.connector &&
			_this.destination == that.destination;
	}
	
	static class VariableInfo {
		
		int occurrences = 0;
		
		// mch -- added to help track aggregation variable
		boolean aggVar = false;
		
		boolean currentAggVar = false;
		boolean contextAgg = false;
		// mch
		
		// a single property may be mapped to
		boolean normalNotExpanded = false;
		boolean normalExpanded = false;
		// in existence tests
		boolean optionalExpanded = false;
		boolean optionalNotExpanded = false;
		
	}
	
	static class PropertyVariables {
			
		String optionalVal;
		String normalVal;
		
		String optionalExpandedPredicate;
		String optionalExpandedVal;
		String normaleExpandedPredicate;
		String normalExpandedVal;
		
	}
	
	private void processTopCriteriaContainersV2(ContainerContext context, VitalGraphCriteriaContainer container, FilterContainer filterContainer, String indent) {
		
		// another attempt to process

		// analyze containers, components, first evaluate, check which values are in use
		
		Map<GraphElement, Map<String, VariableInfo>> variablesHistogram = new HashMap<GraphElement, Map<String, VariableInfo>>();
		
		// place distinct or aggregation properties in the first place ?
		
		if(aggSelectQuery != null) {
			
			VitalSelectAggregationQuery aggQuery = aggSelectQuery;
			
			Map<String, VariableInfo> map = new HashMap<String, VariableInfo>();

			// in aggregation this is a normal 
			VariableInfo vi = new VariableInfo();
			
			vi.aggVar = true;
			
			vi.normalNotExpanded = true;
			
			vi.occurrences ++;
			
			// map.put(aggQuery.getPropertyURI(), vi);
			
			// mch
			if(context.container.getCountArc() == false) {
				
				map.put(aggQuery.getPropertyURI(), vi);

				// System.out.println( "Container Get Count Arc is NOT set." );

				// System.out.println("Current Value of agg var name: " + 	aggregationVarName);

				vi.contextAgg = true;
				
			}
			else {
				
				// System.out.println( "Container Get Count Arc is set." );

				// System.out.println("Current Value of agg var name: " + 	aggregationVarName);

			}
			
			// if(context.sourceVar == null) {
			// 	
			// }

			// if(context.connectorVar == null) {
			// 	
			// }
			
			// mch
			// hack to push var to the destination if present
			// which helps put agg var in the child ARC
			
			if(context.destinationVar == null) {
				
				variablesHistogram.put(GraphElement.Source, map);
			}
			else {
				
				variablesHistogram.put(GraphElement.Destination, map);
			}
			
			// orig:
			// variablesHistogram.put(GraphElement.Source, map);
			
		} else if(distinctSelectQuery != null) {
			
			Map<String, VariableInfo> map = new HashMap<String, VariableInfo>();
			
			VariableInfo vi = new VariableInfo();
			if(distinctSelectQuery.isDistinctExpandProperty()) {
				vi.normalExpanded = true;
			} else {
				vi.normalNotExpanded = true;
			}
			vi.occurrences ++;
			
			map.put(distinctSelectQuery.getPropertyURI(), vi);
			
			// TODO need to adjust this similar to aggregation case to put into the destination?
			variablesHistogram.put(GraphElement.Source, map);
			
		} else if(selectQuery != null) {
			
			List<VitalSortProperty> sortProperties = selectQuery.getSortProperties();
			
			Map<String, VariableInfo> map = new HashMap<String, VariableInfo>();
			
			for(VitalSortProperty vsp : sortProperties) {
				
				String propertyURI = vsp.getPropertyURI();
			
				if(map.containsKey(propertyURI)) throw new RuntimeException("Cannot use same property more than once in sort");
				
				VariableInfo vi = new VariableInfo();
				if(vsp.isExpandProperty()) {
					vi.optionalExpanded = true;
				} else {
					vi.optionalNotExpanded = true;
				}
				
				vi.occurrences ++;
				map.put(propertyURI, vi);
				
			}
			
			variablesHistogram.put(GraphElement.Source, map);
		}
		
		collectHistogram(context, container, variablesHistogram);
		
		Map<GraphElement, Map<String, PropertyVariables>> variablesMaps = new HashMap<GraphElement, Map<String, PropertyVariables>>();
		
		for( Entry<GraphElement, Map<String,VariableInfo>> e : variablesHistogram.entrySet() ) {
		
			Map<String, VariableInfo> value = e.getValue();
			
			Map<String, PropertyVariables> newMap = new HashMap<String, PropertyVariables>();
			
			GraphElement key = e.getKey();
			
			variablesMaps.put(key, newMap);
			
			String parentVar = null;
			
			if(key == GraphElement.Source) {
				
				parentVar = context.sourceVar;
				
			} else if(key == GraphElement.Connector) {
				
				parentVar = context.connectorVar;
				
			} else if(key == GraphElement.Destination) {
				
				parentVar = context.destinationVar;
				
			}
			
			if(parentVar == null) throw new RuntimeException("no parent variable for symbol: " + key.name());
			
			for(Entry<String,VariableInfo> e2 : value.entrySet()) {
				
				String pName = e2.getKey();
				
				//skip URI property
				if(VitalGraphQueryPropertyCriterion.URI.equals(pName)) {
					PropertyVariables pv = new PropertyVariables();
					pv.normalVal = parentVar;
					newMap.put(key.name() + pName, pv);
					continue;
				}
				
				VariableInfo vi = e2.getValue();
				
				PropertyVariables pv = new PropertyVariables();
								
				if( vi.optionalNotExpanded ) {
					String valueVar = "?value" + (++valueIndex);
					builder.append(indent + "OPTIONAL { " + parentVar + " " + predicate(pName) + " " + valueVar + " } . \n");
					pv.optionalVal = valueVar;
				} 
				
				if ( vi.optionalExpanded ) {
					String valueVar = "?value" + (++valueIndex);
					pv.optionalExpandedPredicate = "?predicate" + (++predicateIndex); 
					builder.append(indent + "OPTIONAL { " + parentVar + " " + pv.optionalExpandedPredicate /* predicate(pName)*/ + " " + valueVar + " } . \n");
					pv.optionalExpandedVal = valueVar;
				}
				
				// TESTING
				// this is the case that occurs in agg queries 
				if(vi.aggVar == true && vi.contextAgg == true) { 
					
					if(vi.normalNotExpanded) {
						
						String valueVar = "?value" + (++valueIndex);
						
						// only add to builder at the bottom?
						
						if(vi.currentAggVar == true) {
							
							builder.append(indent + parentVar + " " + predicate(pName) + " " + valueVar + " . \n");
							
							aggregationVarName = valueVar;
							
						}
						
						pv.normalVal = valueVar;
					}
				}
				else {
					
					// leave this case alone
					if(vi.normalNotExpanded) {
						String valueVar = "?value" + (++valueIndex);
						builder.append(indent + parentVar + " " + predicate(pName) + " " + valueVar + " . \n");
						pv.normalVal = valueVar;
					}
					
				}
				
				if(vi.normalExpanded) {
					String valueVar = "?value" + (++valueIndex);
					pv.normaleExpandedPredicate = "?predicate" + (++predicateIndex);
					builder.append(indent + parentVar + " " + pv.normaleExpandedPredicate /*predicate(pName)*/ + " " + valueVar + " . \n");
					pv.normalExpandedVal = valueVar; 
				}
				
				newMap.put(pName, pv);
				
			}
		}
		
		if(selectQuery != null) {
		
			boolean hasStatements = false;
			//check histogram to see if we have at least 1 non optional statement
			for(Map<String, VariableInfo> e : variablesHistogram.values()) {
				
				for(VariableInfo vi : e.values()) {
					if( vi.normalExpanded || vi.normalNotExpanded ) {
						hasStatements = true;
						break;
					}
				}
				
			}
			
			if(!hasStatements) {
				// just insert the artificial line
				builder.append(indent + ind + firstSourceVar + " ?px ?ox .\n");
			}
			
			if(selectQuery.isProjectionOnly()) {
				
				overriddenBindingsString = " ( COUNT ( DISTINCT " + firstSourceVar + " ) AS ?count ) ";
				
			}
		}
		
		// place distinct or aggregation properties in the first place ?
		if(aggSelectQuery != null) {
			
			// TEST override using destination
			
			Map<String, PropertyVariables> map = variablesMaps.get(GraphElement.Destination);

			if(map == null) {
			
				map = variablesMaps.get(GraphElement.Source);
			
			}
			
			// Map<String, PropertyVariables> map = variablesMaps.get(GraphElement.Source);

			// TODO
			// more than one since property URI may not be unique
			// Fix via only inserting the marked one into the map?
			// for delay until after gets set  in class after fully parsing?
			
			// System.out.println("Agg Property URI: " + aggSelectQuery.getPropertyURI());
			
			PropertyVariables pv = map.get( aggSelectQuery.getPropertyURI() );
						
			// is not set yet, perhaps in later pass?
			if(pv == null || aggregationVarName == null) {
			
				// System.out.println("PV is null so far.");
				
			}
			else {
				
				// System.out.println("PV is NOT null.");
	
				// if(pv == null) throw new RuntimeException("Property variables not found: " + aggSelectQuery.getPropertyURI());
			
				String v = pv.normalVal;
			
			
				// override value??
				v = aggregationVarName;

				if(v == null) throw new RuntimeException("Internal: no aggregation property value variable set!");

				AggregationType aggType = aggSelectQuery.getAggregationType();

				if(aggType == AggregationType.count) {
					overriddenBindingsString = " ( COUNT ( " + (aggSelectQuery.isDistinct() ? "DISTINCT" : "") + " " + v + " ) AS ?" + aggType.name() + " )";
				} else if(
						aggType == AggregationType.sum ||
						aggType == AggregationType.average ||
						aggType == AggregationType.min ||
						aggType == AggregationType.max
						) {
					String function = null;
					if(aggType == AggregationType.average) {
						function = "AVG";
					} else {
						function = aggType.name().toUpperCase();
					}
					overriddenBindingsString = " ( " + function + " ( " + v + " ) AS ?" + aggType.name() + " )";
				} else {
					throw new RuntimeException("Unhandled aggregation type: " + aggType); 
				}

				// aggregationValueVariable = "?" + aggType.name();
			}
		} else if(distinctSelectQuery != null) {

			Map<String, PropertyVariables> map = variablesMaps.get(GraphElement.Source);

			PropertyVariables pv = map.get(distinctSelectQuery.getPropertyURI());

			if(pv == null) throw new RuntimeException("Property variables not found: " + distinctSelectQuery.getPropertyURI());

			String v = null;

			if(distinctSelectQuery.isDistinctExpandProperty()) {
				v = pv.normalExpandedVal;
			} else {
				v = pv.normalVal;
			}

			if(v == null) throw new RuntimeException("Internal: no distinct property value variable set!");

			distinctValueVariable = v;

			overriddenBindingsString = "DISTINCT " + v;

		} else if(selectQuery != null) {

			Map<String, PropertyVariables> map = variablesMaps.get(GraphElement.Source);

			for(VitalSortProperty vsp : selectQuery.getSortProperties()) {

				String pURI = vsp.getPropertyURI();

				PropertyVariables pv = map.get(pURI);

				if(pv == null) throw new RuntimeException("Property variables not found for sort property: " + pURI);

				String sortVar = null;

				if(vsp.isExpandProperty()) {
					sortVar = pv.optionalExpandedVal;
				} else {
					sortVar = pv.optionalVal;
				}

				if(sortVar == null) throw new RuntimeException("Sort variable not found for property " + pURI);

				sortVariables.put(pURI, sortVar);	
			}
		}
		
		// giant filter now...
		if(variablesMaps.size() == 0) {
			return;
		}
		
		if(filterContainer != null) {
			
		} else {
			builder.append(indent + "FILTER (\n");
		}
		
		
		List<VitalGraphQueryPropertyCriterion> expandedCriteria = new ArrayList<VitalGraphQueryPropertyCriterion>();
		
		processFilters(context, container, filterContainer, variablesMaps, indent + ind, expandedCriteria);
		
		if(aggSelectQuery != null) {
			
		} else if(distinctSelectQuery != null) {
			if(distinctSelectQuery.getDistinctExpandProperty()) {
				VitalGraphQueryPropertyCriterion c = new VitalGraphQueryPropertyCriterion(distinctSelectQuery.getPropertyURI());
				c.setSymbol(GraphElement.Source);
				c.setComparator(Comparator.EQ);
				expandedCriteria.add(c);
			}
		} else if(selectQuery != null) {
			
			for(VitalSortProperty vsp : selectQuery.getSortProperties()) {
				
				if(vsp.isExpandProperty()) {
					
					VitalGraphQueryPropertyCriterion c = new VitalGraphQueryPropertyCriterion(vsp.getPropertyURI());
					c.setSymbol(GraphElement.Source);
					c.setComparator(Comparator.EQ);
					expandedCriteria.add(c);	
				}
			}
		}
		
		if(expandedCriteria.size() > 0 && filterContainer != null ) {
			
			if(filterContainer.parent == null) throw new RuntimeException("Parent should be set here!");
			
			// wrap it with AND container to be sure
			FilterAND wrapper = new FilterAND(filterContainer.parent);
			wrapper.parent.containers.remove(filterContainer);
			wrapper.containers.add(filterContainer);
			filterContainer.parent = wrapper;
			filterContainer = wrapper;
			
		}
		
		for(VitalGraphQueryPropertyCriterion expanded : expandedCriteria) {
			
			String propURI = expanded.getPropertyURI();
			if(propURI.equals(VitalGraphQueryPropertyCriterion.URI)) continue;
			
			FilterOR orFilter = null;
			
			if(filterContainer != null) {
				orFilter = new FilterOR(filterContainer);
			} else {
				builder.append(indent + "\t && (\n");
			}
			
			PropertyMetadata pm = VitalSigns.get().getPropertiesRegistry().getProperty(propURI);
			
			if(pm == null) throw new RuntimeException("Property metadata not found: " + propURI);
		
			List<PropertyMetadata> subProperties = VitalSigns.get().getPropertiesRegistry().getSubProperties(pm, true);

			boolean first = true;

			boolean optional = expanded.getComparator() == Comparator.EXISTS || expanded.getComparator() == Comparator.NOT_EXISTS;

			Map<String, PropertyVariables> m = variablesMaps.get(expanded.getSymbol());
			
			if(m == null) throw new RuntimeException("Variables histogram not found for symbol: " + expanded.getSymbol());
			
			PropertyVariables pv = m.get(propURI);
			if(pv == null) throw new RuntimeException("No variable info for property: " + propURI);
			
			String predicate = optional ? pv.optionalExpandedPredicate : pv.normaleExpandedPredicate; 
			
			for(PropertyMetadata sub : subProperties) {
				
				if(orFilter != null) {
					
					orFilter.filters.add( predicate + " = " + predicate(sub.getPattern().getURI()) );
					
				} else {
					
					if(first) {
						
						first = false;
						
						builder.append("\t\t    ");
						
					} else {
						
						builder.append("\t\t || ");
						
					}
					
					builder.append( predicate + " = " + predicate(sub.getPattern().getURI()) + "\n");
					
				}
			}
			
			if(filterContainer != null) {
				
			} else {
				
				builder.append(indent + "\t )\n");
			}
		}
		
		// END OF FILTER
		if(filterContainer != null) {
			
		} else {
			builder.append(indent + ")\n");
		}
	}
	
	private void processFilters(ContainerContext context, VitalGraphCriteriaContainer container, FilterContainer filterContainer, Map<GraphElement, Map<String, PropertyVariables>> variablesMaps, String indent, List<VitalGraphQueryPropertyCriterion> expandedCriteria) {

		boolean first = true;
		
		for(VitalGraphQueryElement el : container ) {

			if(filterContainer != null) {
			} else {
				
				builder.append(indent);
			
				if(first) {
					
					first = false;
					
				} else {
					
					builder.append(container.getType() == QueryContainerType.and ? "&& " : "|| ");
				}
			}
			
			if(el instanceof VitalGraphCriteriaContainer) {
				
				if(filterContainer != null) {
					
				} else {
					builder.append("(\n");
				}
				
				VitalGraphCriteriaContainer c = (VitalGraphCriteriaContainer) el;
				
				FilterContainer newContainer = null;
				
				if(filterContainer != null) {
					
					newContainer = c.getType() == QueryContainerType.and ? new FilterAND(filterContainer) : new FilterOR(filterContainer); 
				}
				
				processFilters(context, c, newContainer, variablesMaps, indent + ind, expandedCriteria);

				if(filterContainer != null) {
					
				} else {
					builder.append(")\n");
				}
				
			} else if(el instanceof VitalGraphQueryPropertyCriterion) {
				
				VitalGraphQueryPropertyCriterion pc = (VitalGraphQueryPropertyCriterion) el;
				
				Map<String, PropertyVariables> m = variablesMaps.get(pc.getSymbol());
				
				if(m == null) {
					throw new RuntimeException("No variable mapping: " + pc.getSymbol().name());
				}
				
				if(pc.isExpandProperty()) {
					expandedCriteria.add(pc);
				}
				
				GraphElement symbol = pc.getSymbol();
				
				if(symbol == null) throw new RuntimeException("No graph symbol for property constraint: " + pc);
				
				String symbolVar = null;
				
				if(symbol == GraphElement.Source) {
					symbolVar = context.sourceVar;
					
				} else if(symbol == GraphElement.Connector) {
					symbolVar = context.connectorVar;
					
				} else if(symbol == GraphElement.Destination) {
					symbolVar = context.destinationVar;
					
				} else {
					throw new RuntimeException("Unknown symbol: " + symbol);
				}
				
				Comparator c = pc.getComparator();
				
				PropertyMetadata pm = null;
				
				if(!(pc instanceof VitalGraphQueryTypeCriterion)) {
					
					pm = VitalSigns.get().getPropertiesRegistry().getProperty(pc.getPropertyURI());
					
					if(pm != null) {
						
						if(pm.isMultipleValues()) {
							
							if(pm.getBaseClass() == StringProperty.class && ( c == Comparator.CONTAINS_CASE_INSENSITIVE || c == Comparator.CONTAINS_CASE_SENSITIVE)) {
								
							} else if( c == Comparator.CONTAINS || c == Comparator.NOT_CONTAINS ) {
								
							} else {
								throw new RuntimeException("Multivalue properties may only be queried with CONTAINS/NOT-CONTAINS comparators, or it it's a string multi value property: " + Comparator.CONTAINS_CASE_INSENSITIVE + " or " + Comparator.CONTAINS_CASE_INSENSITIVE);
							}
						}
						
						if( ( c == Comparator.CONTAINS || c == Comparator.NOT_CONTAINS) && !pm.isMultipleValues() ) {
							throw new RuntimeException("CONTAINS/NOT-CONTAINS may only be used for multi-value properties");
						}
						
					}
					
				}
				
				// exists and not exists are turned into simple EXISTS NOT EXISTS pattern now
				
				if( c == Comparator.EXISTS || c == Comparator.NOT_EXISTS ) {
					
					/*
					PropertyVariables pv = m.get(pc.getPropertyURI());
					
					if(pv == null) {
						throw new RuntimeException("No variable mapping: " + pc.getSymbol().name() + " / " + pc.getPropertyURI());
					}
					
					String _val = null;
					if(pc.isExpandProperty()) {
						_val = pv.optionalExpandedVal;
					} else {
						_val = pv.optionalVal;
					}
					if(_val == null) {
						throw new RuntimeException("No optional variable mapping: " + pc.getSymbol().name() + " / " + pc.getPropertyURI());
					}
					
					boolean bound = true;
					if( ( c == Comparator.EXISTS && !pc.isNegative() ) || ( c == Comparator.NOT_EXISTS && pc.isNegative() ) ) {
						
					} else if( ( c == Comparator.NOT_EXISTS && !pc.isNegative() ) || ( c == Comparator.EXISTS && pc.isNegative() ) ) {
						bound = false;
					}
					
					builder.append( ( bound ? "" : " ! " ) + "BOUND( " + _val + " )\n");
					*/
					
					
					//
					// get parent variable
					// this
					
					String innerVar = "?ex" + ++valueIndex;
					
					boolean not = (pc.isNegative() && c == Comparator.EXISTS) || (!pc.isNegative() && c == Comparator.NOT_EXISTS);
					
					String s = ( not ? "NOT " : "") + "EXISTS { " + symbolVar + " " + predicate(pc.getPropertyURI()) + " " + innerVar + " }";
					
					if(filterContainer != null) {
						
						filterContainer.filters.add(s);
						
					} else {
						
						builder.append(s + "\n");
						
					}
					
					continue;					
				} 

				String valueString = null;
				
				String valueStringTyped = null;
				
				boolean literalValue = false;
				
				try {
					
					if(pc.getValue() instanceof URIProperty) {
						
						valueString = uriVal( ((URIProperty)pc.getValue()).get() );
						
					} else {
						
						literalValue = true;

						valueStringTyped = SparqlQueryGenerator.convertValue(pc.getValue(), "xsd");
						
						valueString = "\"" + pc.getValue().toString() + "\"";						
					}
					
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				
				if( c == Comparator.CONTAINS || c == Comparator.NOT_CONTAINS ) {
					
					//
					// get parent variable
					// this
					
					String innerVar = "?ex" + ++valueIndex;
					
					boolean not = (pc.isNegative() && c == Comparator.CONTAINS) || (!pc.isNegative() && c == Comparator.NOT_CONTAINS);
					
					String s = ( not ? "NOT " : "") + "EXISTS { " + symbolVar + " " + predicate(pc.getPropertyURI()) + " " + innerVar + " . " +
							//inner filter
							"FILTER ( " + innerVar + " = " + valueString +" ) }";
					
					if(filterContainer != null) {
						filterContainer.filters.add(s);
						
					} else {
						builder.append(s + "\n");
						
					}

					continue;					
				}
				
				PropertyVariables pv = null;
				
				if(VitalGraphQueryPropertyCriterion.URI.equals(pc.getPropertyURI())) {
					pv = m.get(pc.getSymbol().name() + pc.getPropertyURI());
					
				} else {
					pv = m.get(pc.getPropertyURI());
				}
				
				if(pv == null) {
					throw new RuntimeException("No variable mapping: " + pc.getSymbol().name() + " / " + pc.getPropertyURI());
				}
				
				String _val = null;
				
				if(pc.isExpandProperty()) {
					_val = pv.normalExpandedVal;
					
				} else {
					_val = pv.normalVal;
					
				}
				if(_val == null) {
					throw new RuntimeException("No normal variable mapping: " + pc.getSymbol().name() + " / " + pc.getPropertyURI());
				}
				
				String valueVar = _val;
				
				Class<?> cls = pc.getValue().getClass();
				
				List<Class<?>> classes = comparator2Classes.get(c);
				
				if(classes == null) throw new RuntimeException("comparator unhandled: " + c);
				
				if(classes.size() > 0 && classes.indexOf(cls) < 0) throw new RuntimeException("Cannot use comparator: " + c + " for value of class: " + pc.getValue().getClass().getCanonicalName());
				
				String operator = null;
				
				String completePart = null;
				
				if(c == Comparator.EQ || c == Comparator.CONTAINS) {
					
					if(pc.isNegative()) {
						
						operator = "!=";
						
					} else {
						
						operator = "=";
					}
					
				} else if(c == Comparator.NE) {
					
					if(pc.isNegative()) {
						
						operator = "=";
						
					} else {
						
						operator = "!=";
					}
					
				} else if(c == Comparator.NOT_CONTAINS) {
					
					if(pc.isNegative()) {
						
						operator = "=";
					} else {
						
						operator = "!=";
					}
					
				} else if(c == Comparator.GE || c == Comparator.GT || c == Comparator.LE || c == Comparator.LT){
				
					// use range filter but include negative
					
					if( ( c == Comparator.GE && !pc.isNegative() ) || ( c == Comparator.LT && pc.isNegative() ) ) {
						
						operator = ">=";
						
					} else if( ( c == Comparator.GT && !pc.isNegative() ) || ( c == Comparator.LE && pc.isNegative() ) ) {
						
						operator = ">";
						
					} else if( ( c == Comparator.LE && !pc.isNegative() ) || ( c == Comparator.GT && pc.isNegative() ) ) {
						
						operator = "<=";
						
					} else if( ( c == Comparator.LT && !pc.isNegative() ) || ( c == Comparator.GE && pc.isNegative() ) ) {
						
						operator = "<";
					}
					
				} else if(c == Comparator.CONTAINS_CASE_INSENSITIVE || c == Comparator.CONTAINS_CASE_SENSITIVE) {
				
					String insensitiveFlag = c == Comparator.CONTAINS_CASE_INSENSITIVE ? "i" : "";
					
					
					// mch
					// test bif:contains
					// completePart = (pc.isNegative() ? " (! " : "") + "regex(" + valueVar + ", \"(^|[^a-z]+)" + SparqlQueryGenerator.encodeString((String) pc.getValue()) + "($|[^a-z]+)\", \"" + insensitiveFlag + "m\")" + (pc.isNegative() ? ")": "");
				
					completePart = (pc.isNegative() ? " (! " : "") + "bif:contains(" + valueVar + ",\"'" + SparqlQueryGenerator.encodeString((String) pc.getValue()) + "'\")" + (pc.isNegative() ? ")": "");

					
					
					
				} else if(c == Comparator.REGEXP || c == Comparator.REGEXP_CASE_SENSITIVE) {
					
					String insensitiveFlag = ( c == Comparator.REGEXP ? "i" : "");
					
					completePart = (pc.isNegative() ? "(! " : "") + "regex(" + valueVar + ", \"" + SparqlQueryGenerator.encodeString((String) pc.getValue()) + "\", \"" + insensitiveFlag + "m\")" + (pc.isNegative() ? ")" : "");
					
				} else if(c == Comparator.EQ_CASE_INSENSITIVE ) {
					
					completePart = (pc.isNegative() ? "(! " : "") + "lcase(" + valueVar + ") = \"" + SparqlQueryGenerator.encodeString(((String) pc.getValue()).toLowerCase()) + "\"";
					
				} else {
					
					throw new RuntimeException("Unsupported comparator: " + c);
				}
				
				String s = null;
				
				// TODO
				// virtuoso seems to require converting to STR for equality
				// need to determine if this is for other comparators
				
				// && operator == "=" ?
				// valueStringTyped includes the ^^xsd typing
				
				if(literalValue == true && operator == "=" ) {
				
					s = completePart != null ? completePart : ( 
						"STR(" + valueVar + ")" + " " + operator + " " + valueString
						); 
				}
				else {
					
					if(valueStringTyped != null) {
						
						s = completePart != null ? completePart : ( 
							valueVar + " " + operator + " " + valueStringTyped
						); 	
						
					}
					else {
						s = completePart != null ? completePart : ( 
							valueVar + " " + operator + " " + valueString
						); 
					}
				}
				
				if(filterContainer != null) {
					
					filterContainer.filters.add(s);
					
				} else {
					builder.append(s);
				}
				
			} else {
				throw new RuntimeException("Unexpected object in a criteria container");
			}
		}
	}

	private void collectHistogram(ContainerContext context, VitalGraphCriteriaContainer container,
			Map<GraphElement, Map<String, VariableInfo>> variablesHistogram) {
		
		boolean aggQuery = false;
		
		// look for aggregation property and mark it
		
		String aggPropertyURI = null;
		
		if(aggSelectQuery != null ) {
			
			aggQuery = true;
			
			aggPropertyURI = aggSelectQuery.getPropertyURI();	
		}
		
		for ( VitalGraphQueryElement e : container ) {
			
			if(e instanceof VitalGraphCriteriaContainer ){
				
				collectHistogram(context, (VitalGraphCriteriaContainer) e, variablesHistogram);
				
			} else if( e instanceof VitalGraphQueryPropertyCriterion) {
					
				VitalGraphQueryPropertyCriterion c = (VitalGraphQueryPropertyCriterion) e;
				
				Comparator comp = c.getComparator();
				
				if(comp == Comparator.EXISTS || comp == Comparator.NOT_EXISTS || comp == Comparator.CONTAINS || comp == Comparator.NOT_CONTAINS) {
					continue;
				}
				
				GraphElement symbol = c.getSymbol();
				
				if(symbol == null && ( selectQuery != null || distinctSelectQuery != null || aggSelectQuery != null)) {
					symbol = GraphElement.Source;
					c.setSymbol(symbol);
				}
				
				String var = null;
				if(symbol == GraphElement.Source) {
					
					var = context.sourceVar;
					
				} else if(symbol == GraphElement.Connector) {
					
					var = context.connectorVar;
					
				} else if(symbol == GraphElement.Destination) {
					
					var = context.destinationVar;
				}
				
				if(var == null) throw new RuntimeException("No parent variable for symbol: " + symbol + " - pURI:" + c.getPropertyURI() + " value: " + c.getValue());
				
				Map<String, VariableInfo> m = variablesHistogram.get(symbol);
				
				VariableInfo v = null;
				
				if(m == null) {
					m = new HashMap<String, VariableInfo>();
					variablesHistogram.put(symbol, m);
					
				} else {
					v = m.get(c.getPropertyURI());
					
				}
				
				if(v == null) {
					
					v = new VariableInfo();
					m.put(c.getPropertyURI(), v);
				}
				
				v.occurrences++;
				
				// mch
				if(aggQuery) {
						
					String criterionPropertyURI = c.getPropertyURI();

					if(criterionPropertyURI == aggPropertyURI) {
						
						v.currentAggVar = true;	
					}
				}
				
				if( c.getComparator() == Comparator.EXISTS || c.getComparator() == Comparator.NOT_EXISTS ) {
					
					if(c.isExpandProperty()) {
						v.optionalExpanded = true;
					} else {
						v.optionalNotExpanded = true;
					}
					
				} else {
					
					if(c.isExpandProperty()) {
						v.normalExpanded = true;
					} else {
						v.normalNotExpanded = true;
					}
					
				}
				
				// m.put(c.getPropertyURI(), v != null ? v+1 : 1);
				
			} else {
				throw new RuntimeException("Unexpected graph query element: " + e);
			}
		}	
	}

	public static String toSparqlString(VitalGraphQuery gq) {
		
		GraphQueryImplementation impl = new GraphQueryImplementation(gq, null);
		
		return impl.generateSparqlQuery();	
	}
	
	public static String toSparqlString(VitalSelectQuery sq) {
		
		GraphQueryImplementation impl = new GraphQueryImplementation(sq, null);
		
		return impl.generateSparqlQuery();
		
	}
	
}

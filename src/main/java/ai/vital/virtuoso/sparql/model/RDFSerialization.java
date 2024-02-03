package ai.vital.virtuoso.sparql.model;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import ai.vital.vitalsigns.model.property.GeoLocationProperty;
import ai.vital.vitalsigns.model.property.IProperty;
import ai.vital.vitalsigns.model.property.OtherProperty;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.model.GeoLocationDataType;
import ai.vital.vitalsigns.rdf.RDFDate;
import ai.vital.vitalsigns.utils.StringUtils;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class RDFSerialization {

	// copied from vitalsigns! raw values
	
	public static Object nodeToJavaType(RDFNode node) {

		if(node.isURIResource()) {

			return new URIProperty(node.asResource().getURI());
			
		} else if(node.isLiteral()) {
			
			Literal lit = ((Literal)node);
			
			RDFDatatype dt = lit.getDatatype();
			
//			Literal lit = node.asLiteral();
//			RDFDatatype dt = lit.getDatatype();
			
			if(XSDDatatype.XSDboolean.equals(dt)) {
				return lit.getBoolean();
			} else if(XSDDatatype.XSDdateTime.equals(dt)) {
				return RDFDate.fromXSDString(lit.getLexicalForm());
			} else if(XSDDatatype.XSDdouble.equals(dt)) {
				return lit.getDouble();
			} else if(GeoLocationDataType.theGeoLocationDataType.equals(dt)) {
				return GeoLocationProperty.fromRDFString(lit.getLexicalForm());
			} else if(XSDDatatype.XSDfloat.equals(dt)) {
				return lit.getFloat();
			} else if(XSDDatatype.XSDint.equals(dt)) {
				return lit.getInt();
			} else if(XSDDatatype.XSDlong.equals(dt)) {
				return lit.getLong();
			} else if(StringUtils.isEmpty(lit.getLanguage()) && ( dt == null || XSDDatatype.XSDstring.equals(dt) ) ) {
				return lit.getString();
			} else {
//				throw new RuntimeException("Unknown literal datatype: " + dt.getURI());
				return new OtherProperty(lit.getLexicalForm(), lit.getDatatypeURI(), lit.getLanguage());
			}
			
		} else {
			throw new RuntimeException("Unhandled rdfnode type: " + node.getClass().getCanonicalName() + " - " + node);
		}
		
	}
	
	public static Object valueToJavaValue(Value val) {
		
		if(val instanceof URI) {
			return new URIProperty(((URI)val).stringValue());
		} else if(val instanceof org.openrdf.model.Literal) {
			org.openrdf.model.Literal l = (org.openrdf.model.Literal) val;
			
			URI dt = l.getDatatype();
			if(dt == null) return l.stringValue();
			
			String u = dt != null ? dt.stringValue() : null;
			
			if(XSDDatatype.XSDboolean.getURI().equals(u)) {
				return l.booleanValue();
			} else if(XSDDatatype.XSDdateTime.getURI().equals(u)) {
				return RDFDate.fromXSDString( l.stringValue() );
			} else if(XSDDatatype.XSDdouble.getURI().equals(u)) {
				return l.doubleValue();
			} else if(GeoLocationDataType.theGeoLocationDataType.getURI().equals(u)) {
				return GeoLocationProperty.fromRDFString(l.stringValue());
			} else if(XSDDatatype.XSDfloat.getURI().equals(u)) {
				return l.floatValue();
			} else if(XSDDatatype.XSDint.getURI().equals(u)) {
				return l.intValue();
			} else if(XSDDatatype.XSDlong.getURI().equals(u)) {
				return l.longValue();
			} else if(StringUtils.isEmpty(l.getLanguage()) && ( dt == null || XSDDatatype.XSDstring.getURI().equals(u))) {
				return l.stringValue();
			} else {
				return new OtherProperty(l.getLabel(), u, l.getLanguage());
			}
			
		} else {
			throw new RuntimeException("Unexpected value type: " + val.getClass().getCanonicalName());
		}
		
	}
	
}

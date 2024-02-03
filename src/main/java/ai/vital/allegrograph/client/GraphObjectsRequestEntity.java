package ai.vital.allegrograph.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.apache.commons.httpclient.methods.RequestEntity;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.DomainOntology;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.rdf.RDFFormat;

public class GraphObjectsRequestEntity implements RequestEntity {

	List<GraphObject> objects;
	
	public GraphObjectsRequestEntity(List<GraphObject> objects) {
		super();
		this.objects = objects;
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
	public void writeRequest(OutputStream arg0) throws IOException {

		for(GraphObject g : objects) {
			
			Model m = ModelFactory.createDefaultModel();
			g.toRDF(m);
			
			// try {
			// ontologyIRISetter(g, m);
			// } catch (Exception e) {
			// throw new IOException(e);
			//	}
			
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			m.write(os, RDFFormat.N_TRIPLE.toJenaTypeString());
			arg0.write( os.toByteArray() );
			arg0.write( "\n".getBytes("UTF-8"));
		}
	}
	
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
}

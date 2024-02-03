package ai.vital.allegrograph.client;

/**
 * Simple wrapper for the AGraph dedicated session.
 *
 */
public class AGraphSession {

	private String sessionURL;

	public AGraphSession(String sessionURL) {
		
		super();
		this.sessionURL = sessionURL;
	}

	public String getSessionURL() {
		return sessionURL;
	}
	
}

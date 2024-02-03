package ai.vital.virtuoso.client

class VitalVirtuosoClientManager {
	
	private static Map<String,VitalVirtuosoClient> clientMap = [:]
	
	public static VitalVirtuosoClient getVitalVirtuosoClient(
		String serverName, 
		String user,
		String password,
		String graphName) {
		
		
		// Note: remove this and switch to VitalGraphService
		// there could be N client option on the same graph
		// where this is trying to enforce only one
		
		
		VitalVirtuosoClient client = new VitalVirtuosoClient(
			serverName, 
			user,
			password,
			graphName)
		
		clientMap[graphName] = null
		
		clientMap[graphName] = client
		
		return client
	}
	
	
	public static VitalVirtuosoClient getRegisteredClient(String graphName) {
		
		
		return clientMap[graphName]
		
		
	}
	
	public static void setRegisteredClient(String graphName, VitalVirtuosoClient client) {
		
		clientMap[graphName] = client
		
	}
	
	
	public static void unregisterClient(String graphName) {
		
		
		VitalVirtuosoClient client = clientMap[graphName]
		
		clientMap[graphName] = null
		
		// close client?
		
		
	}
	
	
	
	
}

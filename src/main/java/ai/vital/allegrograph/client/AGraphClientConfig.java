package ai.vital.allegrograph.client;

public class AGraphClientConfig {

	// should end with slash
	
	private String serverURL;
	
	private String catalogName;
	
	private String repositoryName;
	
	private String username;
	
	private String password;

	private Integer connectionsCount;
	
	public AGraphClientConfig(String serverURL, String catalogName,
			String repositoryName, String username, String password, Integer connectionsCount) {
		super();
		this.serverURL = serverURLFilter(serverURL);
		this.catalogName = catalogName;
		this.repositoryName = repositoryName;
		this.username = username;
		this.password = password;
		this.connectionsCount = connectionsCount;
	}

	public String getServerURL() {
		return serverURL;
	}

	public void setServerURL(String serverURL) {
		this.serverURL = serverURLFilter(serverURL);
	}
	
	private String serverURLFilter(String serverURL) {
		if(!serverURL.endsWith("/")) {
			return serverURL + "/";
		}
		return serverURL;
	}

	public String getCatalogName() {
		return catalogName;
	}

	public void setCatalogName(String catalogName) {
		this.catalogName = catalogName;
	}

	public String getRepositoryName() {
		return repositoryName;
	}

	public void setRepositoryName(String repositoryName) {
		this.repositoryName = repositoryName;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Integer getConnectionsCount() {
		return connectionsCount;
	}

	public void setConnectionsCount(Integer connectionsCount) {
		this.connectionsCount = connectionsCount;
	}

}

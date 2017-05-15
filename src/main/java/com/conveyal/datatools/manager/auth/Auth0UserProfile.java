package com.conveyal.datatools.manager.auth;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.ProjectController;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by demory on 1/18/16.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class Auth0UserProfile {
	
	String email;
    String user_id;
    AppMetadata app_metadata;
    
    public static final Logger LOG = LoggerFactory.getLogger(Auth0UserProfile.class);
    private static String AUTH0_DOMAIN = DataManager.getConfigPropertyAsText("AUTH0_DOMAIN");
    private static String AUTH0_API_TOKEN = DataManager.getConfigPropertyAsText("AUTH0_TOKEN");

    public Auth0UserProfile() {
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getEmail() {
        return email;
    }

    public void setApp_metadata(AppMetadata app_metadata) {
        this.app_metadata = app_metadata;
    }

    public AppMetadata getApp_metadata() {
    	return app_metadata;
    }

    @JsonIgnore
    public void setDatatoolsInfo(DatatoolsInfo datatoolsInfo) {
        this.app_metadata.getDatatoolsInfo().setClientId(datatoolsInfo.clientId);
        this.app_metadata.getDatatoolsInfo().setPermissions(datatoolsInfo.permissions);
        this.app_metadata.getDatatoolsInfo().setProjects(datatoolsInfo.projects);
        this.app_metadata.getDatatoolsInfo().setSubscriptions(datatoolsInfo.subscriptions);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AppMetadata {
    	
    	ObjectMapper mapper = new ObjectMapper();
    	@JsonProperty("datatools")
    	List<DatatoolsInfo> datatools;

        public AppMetadata() {
        }

        @JsonIgnore
        public void setDatatoolsInfo(DatatoolsInfo datatools) {
        	if (this.datatools != null)
        		for(int i = 0; i < this.datatools.size(); i++) {
        			if (DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID").equals(this.datatools.get(i).clientId)) {
        				this.datatools.set(i, datatools);
        			}
        		}
        }
        
        @JsonIgnore
        public DatatoolsInfo getDatatoolsInfo() {
        	if (this.datatools != null)
        		for(int i = 0; i < this.datatools.size(); i++) {
        			DatatoolsInfo dt = this.datatools.get(i);
        			if (DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID").equals(dt.clientId)) {
        				return dt;
        			}
        		}
        	return null;
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DatatoolsInfo {
    	
    	@JsonProperty("client_id")
        String clientId;
        Organization[] organizations;
        Project[] projects;
        Permission[] permissions;
        Subscription[] subscriptions;

        public DatatoolsInfo() {
        }

		public DatatoolsInfo(String clientId, Project[] projects, Permission[] permissions, Organization[] organizations, Subscription[] subscriptions) {
            this.clientId = clientId;
            this.projects = projects;
            this.permissions = permissions;
            this.organizations = organizations;
            this.subscriptions = subscriptions;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }
        
        public String getClientId() {
            return clientId;
        }

        public void setProjects(Project[] projects) {
            this.projects = projects;
        }
        
        public Project[] getProjects() {
        	return projects == null ? new Project[0] : projects;
        }
        
        public void setOrganizations(Organization[] organizations) {
            this.organizations = organizations;
        }
        
        public Organization[] getOrganizations() {
        	return organizations == null ? new Organization[0] : organizations;
        			
        }
        
        public void setPermissions(Permission[] permissions) {
            this.permissions = permissions;
        }
        
        public Permission[] getPermissions() {
        	return permissions == null ? new Permission[0] : permissions;
        }

        public void setSubscriptions(Subscription[] subscriptions) {
            this.subscriptions = subscriptions;
        }

        public Subscription[] getSubscriptions() {
        	return subscriptions == null ? new Subscription[0] : subscriptions;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Project {

    	@JsonProperty("project_id")
    	String project_id;
    	@JsonProperty("permissions")
        Permission[] permissions;
        String[] defaultFeeds;

        public Project() {
        }

		public Project(String project_id, Permission[] permissions, String[] defaultFeeds) {
            this.project_id = project_id;
            this.permissions = permissions;
            this.defaultFeeds = defaultFeeds;
        }

        public void setProject_id(String project_id) {
            this.project_id = project_id;
        }

        public void setPermissions(Permission[] permissions) { this.permissions = permissions; }

        public void setDefaultFeeds(String[] defaultFeeds) {
            this.defaultFeeds = defaultFeeds;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Permission {

    	@JsonProperty("type")
        String type;
        String[] feeds;

        public Permission() {
        }

		public Permission(String type, String[] feeds) {
            this.type = type;
            this.feeds = feeds;
        }

        public void setType(String type) {
            this.type = type;
        }

        public void setFeeds(String[] feeds) {
            this.feeds = feeds;
        }
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Organization {
        @JsonProperty("organization_id")
        String organizationId;
        Permission[] permissions;
//        String name;
//        UsageTier usageTier;
//        Extension[] extensions;
//        Date subscriptionDate;
//        String logoUrl;

        public Organization() {
        }

		public Organization(String organizationId, Permission[] permissions) {
            this.organizationId = organizationId;
            this.permissions = permissions;
        }

        public void setOrganizationId(String organizationId) {
            this.organizationId = organizationId;
        }

        public void setPermissions(Permission[] permissions) {
            this.permissions = permissions;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Subscription {

        String type;
        String[] target;

        public Subscription() {
        }

		public Subscription(String type, String[] target) {
            this.type = type;
            this.target = target;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getType() { return type; }

        public void setTarget(String[] target) {
            this.target = target;
        }

        public String[] getTarget() { 
        	return target;
        }
    }

    public int getProjectCount() {
    	if (app_metadata != null && app_metadata.getDatatoolsInfo() != null && app_metadata.getDatatoolsInfo().projects != null)
    		return app_metadata.getDatatoolsInfo().projects.length;
    	return 0;
    }

    public boolean hasProject(String projectID, String organizationId) {
        if (canAdministerApplication())
        	return true;
        if (canAdministerOrganization(organizationId))
        	return true;
    	if (app_metadata != null) {
    		if (app_metadata.getDatatoolsInfo() == null || app_metadata.getDatatoolsInfo().projects == null)
    			return false;
    		for (Project project : app_metadata.getDatatoolsInfo().projects) {
    			if (project.project_id.equals(projectID))
    				return true;
    		}
    	}
        return false;
    }

    public boolean canAdministerApplication() {
    	if (app_metadata != null)
    		if (app_metadata.getDatatoolsInfo() != null && app_metadata.getDatatoolsInfo().permissions != null) {
    			for(Permission permission : app_metadata.getDatatoolsInfo().permissions) {
    				if(permission.type.equals("administer-application")) {
    					return true;
    				}
    			}
    		}
    	return false;
    }

    public boolean canAdministerOrganization() {
    	if (app_metadata != null)
    		if (app_metadata.getDatatoolsInfo() != null && app_metadata.getDatatoolsInfo().organizations != null && app_metadata.getDatatoolsInfo().organizations.length > 0) {
    			Organization org = app_metadata.getDatatoolsInfo().organizations[0];
    			if (org.permissions != null)
    				for (Permission permission : org.permissions) {
    					if(permission.type.equals("administer-organization")) {
    						return true;
    					}
    				}
    		}
    	return false;
    }

    public Organization getAuth0Organization() {
        if(app_metadata.getDatatoolsInfo() != null && app_metadata.getDatatoolsInfo().organizations != null && app_metadata.getDatatoolsInfo().organizations.length != 0) {
            return app_metadata.getDatatoolsInfo().organizations[0];
        }
        return null;
    }

    public String getOrganizationId() {
        Organization org = getAuth0Organization();
        if (org != null) {
            return org.organizationId;
        }
        return null;
    }

    public boolean canAdministerOrganization(String organizationId) {
//      TODO: adapt for specific org
        if (organizationId == null) {
            return false;
        }
        Organization org = getAuth0Organization();
        if (org != null && org.organizationId.equals(organizationId)) {
            for(Permission permission : org.permissions) {
                if(permission.type.equals("administer-organization")) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean canAdministerProject(String projectID, String organizationId) {
        if(canAdministerApplication())
        	return true;
        if(canAdministerOrganization(organizationId))
        	return true;
        if (app_metadata != null && app_metadata.getDatatoolsInfo() != null)
        	for(Project project : app_metadata.getDatatoolsInfo().projects) {
        		if (project.project_id.equals(projectID)) {
        			for(Permission permission : project.permissions) {
        				if(permission.type.equals("administer-project")) {
        					return true;
        				}
        			}
        		}
        	}
        return false;
    }

    public boolean canViewFeed(String organizationId, String projectID, String feedID) {
        if (canAdministerApplication() || canAdministerProject(projectID, organizationId)) {
            return true;
        }
        if (app_metadata != null && app_metadata.getDatatoolsInfo() != null)
        	for(Project project : app_metadata.getDatatoolsInfo().projects) {
        		if (project.project_id.equals(projectID)) {
        			return checkFeedPermission(project, feedID, "view-feed");
        		}
        	}
        return false;
    }

    public boolean canManageFeed(String organizationId, String projectID, String feedID) {
        if (canAdministerApplication() || canAdministerProject(projectID, organizationId)) {
            return true;
        }
        if (app_metadata != null && app_metadata.getDatatoolsInfo() != null)
        	for(Project project : app_metadata.getDatatoolsInfo().projects) {
        		if (project.project_id.equals(projectID)) {
        			return checkFeedPermission(project, feedID, "manage-feed");
        		}
        	}
        return false;
    }

    public boolean canEditGTFS(String organizationId, String projectID, String feedID) {
        if (canAdministerApplication() || canAdministerProject(projectID, organizationId)) {
            return true;
        }
        if (app_metadata != null && app_metadata.getDatatoolsInfo() != null)
        	for(Project project : app_metadata.getDatatoolsInfo().projects) {
        		if (project.project_id.equals(projectID)) {
        			return checkFeedPermission(project, feedID, "edit-gtfs");
        		}
        	}
        return false;
    }

    public boolean canApproveGTFS(String organizationId, String projectID, String feedID) {
        if (canAdministerApplication() || canAdministerProject(projectID, organizationId)) {
            return true;
        }
        if (app_metadata != null && app_metadata.getDatatoolsInfo() != null)
        	for(Project project : app_metadata.getDatatoolsInfo().projects) {
        		if (project.project_id.equals(projectID)) {
        			return checkFeedPermission(project, feedID, "approve-gtfs");
        		}
        	}
        return false;
    }

    public boolean checkFeedPermission(Project project, String feedID, String permissionType) {
    	if (project == null || permissionType == null)
    		return false;
    	
    	String feeds[] = project.defaultFeeds;

        // check for permission-specific feeds
    	for (Permission permission : project.permissions) {
    		if (permission.type.equals(permissionType)) {
                if(permission.feeds != null) {
                    feeds = permission.feeds;
                }
            }
        }

        for(String thisFeedID : feeds) {
            if (thisFeedID.equals(feedID) || thisFeedID.equals("*")) {
                return true;
            }
        }
        return false;
    }

    @JsonIgnore
    public com.conveyal.datatools.manager.models.Organization getOrganization () {
        Organization[] orgs = getApp_metadata().getDatatoolsInfo().organizations;
        if (orgs != null && orgs.length != 0) {
            return orgs[0] != null ? com.conveyal.datatools.manager.models.Organization.get(orgs[0].organizationId) : null;
        }
        return null;
    }

	public void setAdminProject(String id) throws JsonGenerationException, JsonMappingException, IOException {
		LOG.info("[ZAK] setAdminProject");
		
        if (this.getApp_metadata() == null)
        	this.setApp_metadata(new Auth0UserProfile.AppMetadata());
        if (this.app_metadata.datatools == null)
        	this.app_metadata.datatools = new ArrayList<DatatoolsInfo>();
        Auth0UserProfile.DatatoolsInfo dt = this.getApp_metadata().getDatatoolsInfo();
        if (dt == null) {
        	dt = new Auth0UserProfile.DatatoolsInfo();
        	this.app_metadata.datatools.add(dt);
            dt.setClientId(DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID"));
        }
        int projectNumbers = 0;
        if (dt.getProjects() != null)
        	projectNumbers = dt.getProjects().length;
        Auth0UserProfile.Project[] projects = new Auth0UserProfile.Project[projectNumbers+1];
        for (int i = 0; i < projectNumbers; i++)
        	projects[i] = dt.getProjects()[i];
        projects[projectNumbers] = new Auth0UserProfile.Project();
        projects[projectNumbers].setProject_id(id);
        
        Auth0UserProfile.Permission[] permissions = new Auth0UserProfile.Permission[1];
        permissions[0] = new Auth0UserProfile.Permission();
        permissions[0].setType("administer-project");
        projects[projectNumbers].setPermissions(permissions);
        
        dt.setProjects(projects);
        
        this.save();        
	}

	public String update(JsonNode readTree) throws ClientProtocolException, IOException {

        String url = "https://" + AUTH0_DOMAIN + "/api/v2/users/" + URLEncoder.encode(this.getUser_id(), "UTF-8");
        String charset = "UTF-8";

        HttpPatch request = new HttpPatch(url);

        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
        request.setHeader("Accept-Charset", charset);
        request.setHeader("Content-Type", "application/json");

        JsonNode data = readTree.get("data");
        
        String json = "{ \"app_metadata\": { \"datatools\" : " + data + " }}";
        
        HttpEntity entity = new ByteArrayEntity(json.getBytes(charset));
        request.setEntity(entity);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(request);
        
        return EntityUtils.toString(response.getEntity());
	}
	
	public String save() throws JsonGenerationException, JsonMappingException, IOException {
		try {
			ObjectMapper mapper = new ObjectMapper();
			String data2 = "{ \"app_metadata\": " + mapper.writeValueAsString(this.app_metadata) + " }";
			LOG.info("[ZAK] data = "+data2);
			
			String url = "https://" + AUTH0_DOMAIN + "/api/v2/users/" + URLEncoder.encode(this.getUser_id(), "UTF-8");
	        String charset = "UTF-8";
	        
	        HttpPatch request = new HttpPatch(url);
	
	        request.addHeader("Authorization", "Bearer " + AUTH0_API_TOKEN);
	        request.setHeader("Accept-Charset", charset);
	        request.setHeader("Content-Type", "application/json");
	        
	        HttpEntity entity = new ByteArrayEntity(data2.getBytes(charset));
	        request.setEntity(entity);
	
	        HttpClient client = HttpClientBuilder.create().build();
	        HttpResponse response = client.execute(request);
	        
			return EntityUtils.toString(response.getEntity());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	public void setAdminApp() throws JsonGenerationException, JsonMappingException, IOException {
		if (canAdministerApplication())
			return;
		
		LOG.info("[ZAK] setAdminApp");
        if (this.getApp_metadata() == null)
        	this.setApp_metadata(new Auth0UserProfile.AppMetadata());
        if (this.app_metadata.datatools == null)
        	this.app_metadata.datatools = new ArrayList<DatatoolsInfo>();
        Auth0UserProfile.DatatoolsInfo dt = this.getApp_metadata().getDatatoolsInfo();
        if (dt == null) {
        	dt = new Auth0UserProfile.DatatoolsInfo();
        	this.app_metadata.datatools.add(dt);
            dt.setClientId(DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID"));
        }
        
        int permissionNumbers = 0;
        if (dt.getPermissions() != null)
        	permissionNumbers = dt.getPermissions().length;
        Auth0UserProfile.Permission[] perms = new Auth0UserProfile.Permission[permissionNumbers+1];
        for (int i = 0; i < permissionNumbers; i++)
        	perms[i] = dt.getPermissions()[i];
        perms[permissionNumbers] = new Auth0UserProfile.Permission();
        perms[permissionNumbers].setType("administer-application");;
        
        dt.setPermissions(perms);
        
        this.save();        
	}
}

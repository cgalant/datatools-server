package com.conveyal.datatools.manager;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.auth.Auth0Connection;

import com.conveyal.datatools.manager.controllers.DumpController;
import com.conveyal.datatools.manager.controllers.api.*;
import com.conveyal.datatools.editor.controllers.api.*;

import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource;
import com.conveyal.datatools.manager.extensions.transitfeeds.TransitFeedsFeedResource;
import com.conveyal.datatools.manager.extensions.transitland.TransitLandFeedResource;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.persistence.TestThing;
import com.conveyal.datatools.manager.utils.CorsFilter;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.api.GraphQLMain;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import com.mongodb.client.result.DeleteResult;
import org.apache.commons.io.Charsets;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.utils.IOUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.mongodb.client.model.Filters.eq;
import static spark.Spark.*;

public class DataManager {

    public static final Logger LOG = LoggerFactory.getLogger(DataManager.class);

    public static JsonNode envConfig;
    public static JsonNode serverConfig;

    public static JsonNode gtfsConfig;
    public static JsonNode gtfsPlusConfig;

    // FIXME: why are the following Maps keyed on Strings? Should we define enums instead, or just not have keys and use a List/Set?

    public static final Map<String, ExternalFeedResource> feedResources = new HashMap<>();

    public static ConcurrentHashMap<String, ConcurrentHashSet<MonitorableJob>> userJobsMap = new ConcurrentHashMap<>();

    public static Map<String, ScheduledFuture> autoFetchMap = new HashMap<>();
    public final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());


    // The heavy executor should contain long-lived CPU-intensive tasks (e.g. feed loading/validation).
    public static Executor heavyExecutor = Executors.newFixedThreadPool(4); // Runtime.getRuntime().availableProcessors()

    // The light executor is for tasks for things that should finish quickly (e.g. email notifications).
    public static Executor lightExecutor = Executors.newSingleThreadExecutor();

    public static String feedBucket;
    public static String awsRole;
    public static String bucketFolder;

//    public final AmazonS3Client s3Client;
    public static boolean useS3;
    public static final String API_PREFIX = "/api/manager/";
    public static final String EDITOR_API_PREFIX = "/api/editor/";
    public static final String publicPath = "(" + DataManager.API_PREFIX + "|" + DataManager.EDITOR_API_PREFIX + ")public/.*";
    public static final String DEFAULT_ENV_CONFIG = "configurations/default/env.yml";
    public static final String DEFAULT_SERVER_CONFIG = "configurations/default/server.yml";

    public static DataSource GTFS_DATA_SOURCE;
//    public static Persistence persistence;

    public static void main(String[] args) throws IOException {


        // Load the two YAML configuration files
        loadConfig(args);

        // Set the server port number based on the configuration loaded above
        int port = getConfigPropertyAsInt("application.port", 4000);
        port(port);

        useS3 = "true".equalsIgnoreCase(getConfigPropertyAsText("application.data.use_s3_storage"));

        GTFS_DATA_SOURCE = GTFS.createDataSource(
                getConfigPropertyAsText("GTFS_DATABASE_URL"),
                getConfigPropertyAsText("GTFS_DATABASE_USER"),
                getConfigPropertyAsText("GTFS_DATABASE_PASSWORD")
        );

        feedBucket = getConfigPropertyAsText("application.data.gtfs_s3_bucket");
        awsRole = getConfigPropertyAsText("application.data.aws_role");
        bucketFolder = FeedStore.s3Prefix;

        // Register the GET and POST endpoints for the GraphQL GTFS API with Spark
        GraphQLMain.initialize(GTFS_DATA_SOURCE, API_PREFIX);
        LOG.info("Initialized gtfs-api at localhost:port{}", API_PREFIX);

        Persistence.initialize();

        // initialize map of auto fetched projects
        for (Project project : Persistence.projects.getAll()) {
            if (project.autoFetchFeeds) {
                ScheduledFuture scheduledFuture = ProjectController.scheduleAutoFeedFetch(project, 1);
                autoFetchMap.put(project.id, scheduledFuture);
            }
        }

        registerRoutes();
        registerExternalResources();
    }

    private static void registerRoutes() throws IOException {
        CorsFilter.apply();

        // core controllers
        ProjectController.register(API_PREFIX);
        FeedSourceController.register(API_PREFIX);
        FeedVersionController.register(API_PREFIX);
        RegionController.register(API_PREFIX);
        NoteController.register(API_PREFIX);
        StatusController.register(API_PREFIX);
        OrganizationController.register(API_PREFIX);

        // Editor routes
        if (isModuleEnabled("editor")) {
            String gtfs = IOUtils.toString(DataManager.class.getResourceAsStream("/gtfs/gtfs.yml"));
            gtfsConfig = yamlMapper.readTree(gtfs);
            AgencyController.register(EDITOR_API_PREFIX);
            CalendarController.register(EDITOR_API_PREFIX);
            RouteController.register(EDITOR_API_PREFIX);
            RouteTypeController.register(EDITOR_API_PREFIX);
            ScheduleExceptionController.register(EDITOR_API_PREFIX);
            StopController.register(EDITOR_API_PREFIX);
            TripController.register(EDITOR_API_PREFIX);
            TripPatternController.register(EDITOR_API_PREFIX);
            SnapshotController.register(EDITOR_API_PREFIX);
            FeedInfoController.register(EDITOR_API_PREFIX);
            FareController.register(EDITOR_API_PREFIX);
//            GisController.register(EDITOR_API_PREFIX);
        }

        // log all exceptions to system.out
        exception(Exception.class, (e, req, res) -> LOG.error("error", e));

        // module-specific controllers
        if (isModuleEnabled("deployment")) {
            DeploymentController.register(API_PREFIX);
        }
        if (isModuleEnabled("gtfsapi")) {
            GtfsApiController.register(API_PREFIX);
        }
        if (isModuleEnabled("gtfsplus")) {
            GtfsPlusController.register(API_PREFIX);
            URL gtfsplus = DataManager.class.getResource("/gtfs/gtfsplus.yml");
            gtfsPlusConfig = yamlMapper.readTree(Resources.toString(gtfsplus, Charsets.UTF_8));
        }
        if (isModuleEnabled("user_admin")) {
            UserController.register(API_PREFIX);
        }
        if (isModuleEnabled("dump")) {
            DumpController.register("/");
        }

        before(EDITOR_API_PREFIX + "secure/*", ((request, response) -> {
            Auth0Connection.checkUser(request);
            Auth0Connection.checkEditPrivileges(request);
        }));

        before(API_PREFIX + "secure/*", (request, response) -> {
            if(request.requestMethod().equals("OPTIONS")) return;
            Auth0Connection.checkUser(request);
        });

        // return "application/json" for all API routes
        after(API_PREFIX + "*", (request, response) -> {
//            LOG.info(request.pathInfo());
            response.type("application/json");
            response.header("Content-Encoding", "gzip");
        });
        // load index.html
        InputStream stream = DataManager.class.getResourceAsStream("/public/index.html");
        String index = IOUtils.toString(stream).replace("${S3BUCKET}", getConfigPropertyAsText("application.assets_bucket"));
        stream.close();

        // return 404 for any api response that's not found
        get(API_PREFIX + "*", (request, response) -> {
            halt(404, SparkUtils.formatJSON("Unknown error occurred.", 404));
            return null;
        });

        // return index.html for any sub-directory
        get("/*", (request, response) -> {
            response.type("text/html");
            return index;
        });
    }

    public static boolean hasConfigProperty(String name) {
        // try the server config first, then the main config
        boolean fromServerConfig = hasConfigProperty(serverConfig, name);
        if(fromServerConfig) return fromServerConfig;

        return hasConfigProperty(envConfig, name);
    }

    public static boolean hasConfigProperty(JsonNode config, String name) {
        String parts[] = name.split("\\.");
        JsonNode node = config;
        for(int i = 0; i < parts.length; i++) {
            if(node == null) return false;
            node = node.get(parts[i]);
        }
        return node != null;
    }

    /**
     * Try to get a configuration JSON node specified with a dot-separated path. First search in the server.yml config,
     * then fall back on the env.yml config. FIXME why do we even have two separate config files then?
     */
    public static JsonNode getConfigProperty (String name) {
        JsonNode configItem = getConfigProperty(serverConfig, name);
        if (configItem == null) configItem = getConfigProperty(envConfig, name);
        if (configItem == null) LOG.warn("Config property {} not found in either server or env config file.", name);
        return configItem;
    }

    /**
     * @param config a JSON node, the root of a tree within which to search for the config key
     * @param name a hierarchical, dot-separated path to the config key to be fetched within the JSON node tree
     * @return the found JSON node, or null if the node is not found.
     */
    public static JsonNode getConfigProperty(JsonNode config, String name) {
        // Descend the tree rooted at the supplied config node one level at a time.
        JsonNode node = config;
        for (String pathSegment : name.split("\\.")) {
            node = node.get(pathSegment);
            // Missing config warnings are issued in the caller, do not do so here to avoid cluttering the logs.
            if (node == null) return null;
        }
        return node;
    }

    /**
     * Fetch a configuration element expected to be text, handling the case where it is missing by returning null.
     */
    public static String getConfigPropertyAsText (String name) {
        JsonNode node = getConfigProperty(name);
        if (node != null) return node.asText();
        else return null;
    }

    /**
     * Fetch a configuration element expected to be an integer,
     * handling the case where it is missing by returning a default.
     */
    public static int getConfigPropertyAsInt (String name, int defaultValue) {
        JsonNode node = getConfigProperty(name);
        if (node != null) return node.asInt();
        else return defaultValue;
    }

    public static boolean isModuleEnabled(String moduleName) {
        return hasConfigProperty("modules." + moduleName) && "true".equals(getConfigPropertyAsText("modules." + moduleName + ".enabled"));
    }

    public static boolean isExtensionEnabled(String extensionName) {
        return hasConfigProperty("extensions." + extensionName) && "true".equals(getConfigPropertyAsText("extensions." + extensionName + ".enabled"));
    }

    private static void registerExternalResources() {

        if (isExtensionEnabled("mtc")) {
            LOG.info("Registering MTC Resource");
            registerExternalResource(new MtcFeedResource());
        }

        if (isExtensionEnabled("transitland")) {
            LOG.info("Registering TransitLand Resource");
            registerExternalResource(new TransitLandFeedResource());
        }

        if (isExtensionEnabled("transitfeeds")) {
            LOG.info("Registering TransitFeeds Resource");
            registerExternalResource(new TransitFeedsFeedResource());
        }
    }

    /**
     * Based on command line arguments (if provided), open env and server config files and convert them from YAML to
     * internal representation as Jackson JSON node trees.
     */
    private static void loadConfig (String[] args) throws IOException {
        String envConfigPath = DEFAULT_ENV_CONFIG;
        String serverConfigPath = DEFAULT_SERVER_CONFIG;
        if (args.length == 0) {
            LOG.warn("No command line arguments specified, using default configuration file paths.");
        } else {
            if (args.length != 2) {
                LOG.error("Wrong number of command line arguments. Exiting.");
                System.exit(-1);
            }
            envConfigPath = args[0];
            serverConfigPath = args[1];
        }
        LOG.info("Loading env config YAML from path: {}", envConfigPath);
        envConfig = yamlMapper.readTree(new FileInputStream(new File(envConfigPath)));
        LOG.info("Loading server config YAML from path: {}", serverConfigPath);
        serverConfig = yamlMapper.readTree(new FileInputStream(new File(serverConfigPath)));
    }

    private static void registerExternalResource(ExternalFeedResource resource) {
        feedResources.put(resource.getResourceType(), resource);
    }

}

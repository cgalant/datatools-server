package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.common.utils.Consts;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.FetchProjectFeedsJob;
import com.conveyal.datatools.manager.jobs.MakePublicJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import spark.Request;
import spark.Response;

import javax.servlet.http.HttpServletResponse;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithError;
import static com.conveyal.datatools.manager.DataManager.publicPath;
import static spark.Spark.*;

/**
 * Handlers for HTTP API requests that affect Projects.
 * These methods are mapped to API endpoints by Spark.
 * TODO we could probably have one generic controller for all data types, and use path elements from the URL to route to different typed persistence instances.
 */
@SuppressWarnings({"unused", "ThrowableNotThrown"})
public class ProjectController {

    // TODO We can probably replace this with something from Mongo so we use one JSON serializer / deserializer throughout
    public static JsonManager<Project> json =
            new JsonManager<>(Project.class, JsonViews.UserInterface.class);

    public static final Logger LOG = LoggerFactory.getLogger(ProjectController.class);

    /**
     * @return a list of all projects that are public or visible given the current user and organization.
     */
    private static Collection<Project> getAllProjects(Request req, Response res) throws JsonProcessingException {
        Auth0UserProfile userProfile = req.attribute("user");
        // TODO: move this filtering into database query to reduce traffic / memory
        return Persistence.projects.getAll().stream()
                .filter(p -> req.pathInfo().matches(publicPath) || userProfile.hasProject(p.id, p.organizationId))
                .map(p -> checkProjectPermissions(req, p, "view"))
                .collect(Collectors.toList());
    }

    /**
     * @return a Project object for the UUID included in the request.
     */
    private static Project getProject(Request req, Response res) {
        return requestProjectById(req, "view");
    }

    /**
     * Create a new Project and store it, setting fields according to the JSON in the request body.
     * @return the newly created Project with all the supplied fields, as it appears in the database.
     */
    private static Project createProject(Request req, Response res) {
        // TODO error handling when request is bogus
        // TODO factor out user profile fetching, permissions checks etc.
        Auth0UserProfile userProfile = req.attribute("user");
        Document newProjectFields = Document.parse(req.body());
        String organizationId = (String) newProjectFields.get("organizationId");
        boolean allowedToCreate = userProfile.canAdministerApplication() || userProfile.canAdministerOrganization(organizationId);
        // Data manager can operate without organizations for now, so we (hackishly/insecurely) deactivate permissions here
        if (organizationId == null) allowedToCreate = true;
        if (allowedToCreate) {
            Project newlyStoredProject = Persistence.projects.create(req.body());
            return newlyStoredProject;
        } else {
            haltWithError(403, "Not authorized to create a project on organization " + organizationId);
            return null;
        }
    }

    /**
     * Update fields in the Project with the given UUID. The fields to be updated are supplied as JSON in the request
     * body.
     * @return the Project as it appears in the database after the update.
     */
    private static Project updateProject(Request req, Response res) throws IOException {
        // Fetch the project once to check permissions
        requestProjectById(req, "manage");
        try {
            String id = req.params("id");
            Document updateDocument = Document.parse(req.body());
            Project updatedProject = Persistence.projects.update(id, req.body());
            // Catch updates to auto-fetch params, and update the autofetch schedule accordingly.
            // TODO factor out into generic update hooks, or at least separate method
            if (updateDocument.containsKey("autoFetchHour")
                    || updateDocument.containsKey("autoFetchMinute")
                    || updateDocument.containsKey("autoFetchFeeds")
                    || updateDocument.containsKey("defaultTimeZone")) {
                // If auto fetch flag is turned on
                if (updatedProject.autoFetchFeeds){
                    ScheduledFuture fetchAction = scheduleAutoFeedFetch(updatedProject, 1);
                    DataManager.autoFetchMap.put(updatedProject.id, fetchAction);
                } else{
                    // otherwise, cancel any existing task for this id
                    cancelAutoFetch(updatedProject.id);
                }
            }
            return updatedProject;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400, SparkUtils.formatJSON("Error updating project"));
            return null;
        }
    }

    /**
     * Delete the project for the UUID given in the request.
     */
    private static Project deleteProject(Request req, Response res) throws IOException {
        // Fetch project first to check permissions, and so we can return the deleted project after deletion.
        Project project = requestProjectById(req, "manage");
        boolean successfullyDeleted = Persistence.projects.removeById(req.params("id"));
        if (!successfullyDeleted) {
            halt(400, SparkUtils.formatJSON("Did not delete project."));
        }
        return project;
    }

    /**
     * Manually fetch a feed all feeds in the project as a one-off operation, when the user clicks a button to request it.
     */
    public static Boolean fetch(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        Project p = requestProjectById(req, "manage");
        FetchProjectFeedsJob fetchProjectFeedsJob = new FetchProjectFeedsJob(p, userProfile.getUser_id());
        // This job is runnable because sometimes we schedule the task for a later time, but here we call it immediately
        // because it is short lived and just cues up more work.
        fetchProjectFeedsJob.run();
        return true;
    }

    /**
     * Public helper function that returns the requested object if the user has permissions for the specified action.
     * FIXME why can't we do this checking by passing in the project ID rather than the whole request?
     * FIXME: eliminate all stringly typed variables (action)
     * @param req spark Request object from API request
     * @param action action type (either "view" or "manage")
     * @return requested project
     */
    private static Project requestProjectById (Request req, String action) {
        String id = req.params("id");
        if (id == null) {
            halt(SparkUtils.formatJSON("Please specify id param", 400));
        }
        return checkProjectPermissions(req, Persistence.projects.getById(id), action);
    }

    /**
     * Given a project object, this checks the user's permissions to take some specific action on it.
     * If the user does not have permission the Spark request is halted with an error.
     * TODO: remove all Spark halt calls from data manipulation functions, API implementation is leaking into data model
     * If the user does have permission we return the same project object that was input, but with the feedSources nulled out.
     * In the special case that the user is not logged in and is therefore only looking at public objects, the feed
     * sources list is replaced with one that only contains publicly visible feed sources.
     * This is because the UI only uses Project objects with embedded feedSources in the landing page, nowhere else.
     * That fetch with embedded feedSources should be done with something like GraphQL generating multiple backend
     * fetches, not with a field that's only populated and returned in special cases.
     * FIXME: this is a method with side effects and no clear single purpose, in terms of transformation of input to output.
     */
    private static Project checkProjectPermissions(Request req, Project project, String action) {

        Auth0UserProfile userProfile = req.attribute("user");
        // Check if request was made by a user that is not logged in
        boolean publicFilter = req.pathInfo().matches(publicPath);

        // check for null project
        if (project == null) {
            halt(400, SparkUtils.formatJSON("Project ID does not exist", 400));
            return null;
        }

        boolean authorized;
        switch (action) {
            // TODO: limit create action to app/org admins? see code currently in createProject.
//            case "create":
//                authorized = userProfile.canAdministerOrganization(p.organizationId);
//                break;
            case "manage":
                authorized = userProfile.canAdministerProject(project.id, project.organizationId);
                break;
            case "view":
                // request only authorized if not via public path and user can view
                authorized = !publicFilter && userProfile.hasProject(project.id, project.organizationId);
                break;
            default:
                authorized = false;
                break;
        }

        // If the user is not logged in, include only public feed sources
        if (publicFilter){
            project.feedSources = project.retrieveProjectFeedSources().stream()
                    .filter(fs -> fs.isPublic)
                    .collect(Collectors.toList());
        } else {
            project.feedSources = null;
            if (!authorized) {
                halt(403, SparkUtils.formatJSON("User not authorized to perform action on project", 403));
                return null;
            }
        }
        // if we make it here, user has permission and this is a valid project.
        return project;
    }

    /**
     * Spark request handler that merges all the feeds in the Project into a single feed and returns them as the HTTP
     * response. FIXME: this is being done synchronously in a HTTP request handler thread, make an async job.
     * FIXME: logic to merge GTFS feeds should be moved out of ProjectController, which should only include HTTP controllers
     */
    private static HttpServletResponse downloadMergedFeed(Request req, Response res) throws IOException {
        Project p = requestProjectById(req, "view");

        // retrieveById feed sources in project
        Collection<FeedSource> feeds = p.retrieveProjectFeedSources();

        // create temp merged zip file to add feed content to
        File mergedFile;
        try {
            mergedFile = File.createTempFile(p.id + "-merged", ".zip");
            mergedFile.deleteOnExit();

        } catch (IOException e) {
            LOG.error("Could not create temp file");
            e.printStackTrace();
            halt(400, SparkUtils.formatJSON("Unknown error while merging feeds.", 400));
            return null;
        }

        // create the zipfile
        ZipOutputStream out;
        try {
            out = new ZipOutputStream(new FileOutputStream(mergedFile));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        LOG.info("Created project merge file: " + mergedFile.getAbsolutePath());

        // Map of feed versions to zip files containing those feeds
        Map<FeedSource, ZipFile> feedSourceMap = new HashMap<>();

        // Collect zipFiles for each feedSource before merging tables
        for (FeedSource fs : feeds) {
            // check if feed source has version (use latest)
            FeedVersion version = fs.retrieveLatest();
            if (version == null) {
                LOG.info("Skipping {} because it has no feed versions", fs.name);
                continue;
            }
            // modify feed version to use prepended feed id
            LOG.info("Will include feed '{}' to merged zip.", fs.name);
            try {
                File file = version.retrieveGtfsFile();
                ZipFile zipFile = new ZipFile(file);
                feedSourceMap.put(fs, zipFile);
            } catch(Exception e) {
                e.printStackTrace();
                LOG.error("Zipfile for version {} not found", version.id);
            }
        }

        // loop through GTFS tables
        for(int i = 0; i < DataManager.gtfsConfig.size(); i++) {
            JsonNode tableNode = DataManager.gtfsConfig.get(i);
            byte[] tableOut = mergeTables(tableNode, feedSourceMap);

            // if at least one feed has the table, include it
            if (tableOut != null) {
                String tableName = tableNode.get("name").asText();

                // create entry for zip file
                ZipEntry tableEntry = new ZipEntry(tableName);
                out.putNextEntry(tableEntry);
                LOG.info("Writing {} to merged feed", tableEntry.getName());
                out.write(tableOut);
                out.closeEntry();
            }
        }
        out.close();

        // Deliver zipfile
        res.raw().setContentType("application/octet-stream");
        res.raw().setHeader("Content-Disposition", "attachment; filename=" + mergedFile.getName());

        try {
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(res.raw().getOutputStream());
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(mergedFile));

            byte[] buffer = new byte[1024];
            int len;
            while ((len = bufferedInputStream.read(buffer)) > 0) {
                bufferedOutputStream.write(buffer, 0, len);
            }

            bufferedOutputStream.flush();
            bufferedOutputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
            halt(500, SparkUtils.formatJSON("Error serving GTFS file"));
        }

        return res.raw();
    }

    /**
     * Merge the specified table across multiple GTFS feeds. Used by the endpoint that returns a merged feed made up
     * of all feeds in the project.
     * FIXME: logic to merge GTFS feeds should be moved out of ProjectController, which should only include HTTP controllers
     * @param tableNode tableNode to merge
     * @param feedSourceMap map of feedSources to zipFiles from which to extract the .txt tables
     * @return single merged table for feeds
     */
    private static byte[] mergeTables(JsonNode tableNode, Map<FeedSource, ZipFile> feedSourceMap) {

        String tableName = tableNode.get("name").asText();
        ByteArrayOutputStream tableOut = new ByteArrayOutputStream();

        ArrayNode fieldsNode = (ArrayNode) tableNode.get("fields");
        List<String> headers = new ArrayList<>();
        for (int i = 0; i < fieldsNode.size(); i++) {
            JsonNode fieldNode = fieldsNode.get(i);
            String fieldName = fieldNode.get("name").asText();
            Boolean notInSpec = fieldNode.has("datatools") && fieldNode.get("datatools").asBoolean();
            if (notInSpec) {
                fieldsNode.remove(i);
            }
            headers.add(fieldName);
        }

        try {
            // write headers to table
            tableOut.write(String.join(",", headers).getBytes());
            tableOut.write("\n".getBytes());

            // iterate over feed source to zipfile map
            for ( Map.Entry<FeedSource, ZipFile> mapEntry : feedSourceMap.entrySet()) {
                FeedSource fs = mapEntry.getKey();
                ZipFile zipFile = mapEntry.getValue();
                final Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while (entries.hasMoreElements()) {
                    final ZipEntry entry = entries.nextElement();
                    if(tableName.equals(entry.getName())) {
                        LOG.info("Adding {} table for {}", entry.getName(), fs.name);

                        InputStream inputStream = zipFile.getInputStream(entry);

                        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                        String line = in.readLine();
                        String[] fields = line.split(",");

                        List<String> fieldList = Arrays.asList(fields);

                        // iterate over rows in table
                        while((line = in.readLine()) != null) {
                            String[] newValues = new String[fieldsNode.size()];
                            String[] values = line.split(Consts.COLUMN_SPLIT, -1);
                            if (values.length == 1) {
                                LOG.warn("Found blank line. Skipping...");
                                continue;
                            }
                            for(int v = 0; v < fieldsNode.size(); v++) {
                                JsonNode fieldNode = fieldsNode.get(v);
                                String fieldName = fieldNode.get("name").asText();

                                // retrieveById index of field from GTFS spec as it appears in feed
                                int index = fieldList.indexOf(fieldName);
                                String val = "";
                                try {
                                    index = fieldList.indexOf(fieldName);
                                    if(index != -1) {
                                        val = values[index];
                                    }
                                } catch (ArrayIndexOutOfBoundsException e) {
                                    LOG.warn("Index {} out of bounds for file {} and feed {}", index, entry.getName(), fs.name);
                                    continue;
                                }

                                String fieldType = fieldNode.get("inputType").asText();

                                // if field is a gtfs identifier, prepend with feed id/name
                                if (fieldType.contains("GTFS") && !val.isEmpty()) {
                                    newValues[v] = fs.name + ":" + val;
                                }
                                else {
                                    newValues[v] = val;
                                }
                            }
                            String newLine = String.join(",", newValues);

                            // write line to table (plus new line char)
                            tableOut.write(newLine.getBytes());
                            tableOut.write("\n".getBytes());
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Error merging feed sources: {}", feedSourceMap.keySet().stream().map(fs -> fs.name).collect(Collectors.toList()).toString());
            halt(400, SparkUtils.formatJSON("Error merging feed sources", 400, e));
        }
        return tableOut.toByteArray();
    }

    /**
     * Copy all the latest feed versions for all public feed sources in this project to a bucket on S3.
     * Updates the index.html document that serves as a listing of those objects on S3.
     * This is often referred to as "deploying" the project.
     */
    private static boolean publishPublicFeeds(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        if (id == null) {
            halt(400, "must provide project id!");
        }
        Project p = Persistence.projects.getById(id);
        if (p == null) {
            halt(400, "no such project!");
        }
        // Run this as a synchronous job; if it proves to be too slow we will change to asynchronous.
        new MakePublicJob(p, userProfile.getUser_id()).run();
        return true;
    }

    /**
     * Spark endpoint to synchronize this project's feed sources with another website or service that maintains an
     * index of GTFS data. This action is triggered manually by a UI button and for now never happens automatically.
     * An ExternalFeedResource of the specified type must be present in DataManager.feedResources
     */
    private static Project thirdPartySync(Request req, Response res) throws Exception {
        Auth0UserProfile userProfile = req.attribute("user");
        String id = req.params("id");
        Project proj = Persistence.projects.getById(id);

        String syncType = req.params("type");

        if (!userProfile.canAdministerProject(proj.id, proj.organizationId)) {
            halt(403);
        }

        LOG.info("syncing with third party " + syncType);
        if(DataManager.feedResources.containsKey(syncType)) {
            DataManager.feedResources.get(syncType).importFeedsForProject(proj, req.headers("Authorization"));
            return proj;
        }

        halt(404);
        return null;
    }

    /**
     * Schedule an action that fetches all the feeds in the given project according to the autoFetch fields of that project.
     * Currently feeds are not auto-fetched independently, they must be all fetched together as part of a project.
     * This method is called when a Project's auto-fetch settings are updated, and when the system starts up to populate
     * the auto-fetch scheduler.
     */
    public static ScheduledFuture scheduleAutoFeedFetch (Project project, int intervalInDays) {
        TimeUnit minutes = TimeUnit.MINUTES;
        try {
            // First cancel any already scheduled auto fetch task for this project id.
            cancelAutoFetch(project.id);

            ZoneId timezone;
            try {
                timezone = ZoneId.of(project.defaultTimeZone);
            }catch(Exception e){
                timezone = ZoneId.of("America/New_York");
            }
            LOG.info("Scheduling auto-fetch for projectID: {}", project.id);

            // NOW in default timezone
            ZonedDateTime now = ZonedDateTime.ofInstant(Instant.now(), timezone);

            // Scheduled start time
            ZonedDateTime startTime = LocalDateTime.of(LocalDate.now(),
                    LocalTime.of(project.autoFetchHour, project.autoFetchMinute)).atZone(timezone);
            LOG.info("Now: {}", now.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
            LOG.info("Scheduled start time: {}", startTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));

            // Get diff between start time and current time
            long diffInMinutes = (startTime.toEpochSecond() - now.toEpochSecond()) / 60;
            long delayInMinutes;
            if ( diffInMinutes >= 0 ){
                delayInMinutes = diffInMinutes; // delay in minutes
            }
            else{
                delayInMinutes = 24 * 60 + diffInMinutes; // wait for one day plus difference (which is negative)
            }

            LOG.info("Auto fetch begins in {} hours and runs every {} hours", String.valueOf(delayInMinutes / 60.0), TimeUnit.DAYS.toHours(intervalInDays));

            // system is defined as owner because owner field must not be null
            FetchProjectFeedsJob fetchProjectFeedsJob = new FetchProjectFeedsJob(project, "system");
            return DataManager.scheduler.scheduleAtFixedRate(fetchProjectFeedsJob,
                    delayInMinutes, TimeUnit.DAYS.toMinutes(intervalInDays), minutes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Cancel an existing auto-fetch job that is scheduled for the given project ID.
     * There is only one auto-fetch job per project, not one for each feedSource within the project.
     */
    private static void cancelAutoFetch(String projectId){
        Project p = Persistence.projects.getById(projectId);
        if ( p != null && DataManager.autoFetchMap.get(p.id) != null) {
            LOG.info("Cancelling auto-fetch for projectID: {}", p.id);
            DataManager.autoFetchMap.get(p.id).cancel(true);
        }
    }

    /**
     * This connects all the above HTTP API handlers to URL paths (registers them with the Spark framework).
     * A bit too static/global for an OO language, but that's how Spark works.
     */
    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/project/:id", ProjectController::getProject, json::write);
        get(apiPrefix + "secure/project", ProjectController::getAllProjects, json::write);
        post(apiPrefix + "secure/project", ProjectController::createProject, json::write);
        put(apiPrefix + "secure/project/:id", ProjectController::updateProject, json::write);
        delete(apiPrefix + "secure/project/:id", ProjectController::deleteProject, json::write);
        get(apiPrefix + "secure/project/:id/thirdPartySync/:type", ProjectController::thirdPartySync, json::write);
        post(apiPrefix + "secure/project/:id/fetch", ProjectController::fetch, json::write);
        post(apiPrefix + "secure/project/:id/deployPublic", ProjectController::publishPublicFeeds, json::write);
        get(apiPrefix + "public/project/:id/download", ProjectController::downloadMergedFeed);
        get(apiPrefix + "public/project/:id", ProjectController::getProject, json::write);
        get(apiPrefix + "public/project", ProjectController::getAllProjects, json::write);
    }

}

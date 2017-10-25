package com.conveyal.datatools;

import com.conveyal.datatools.manager.DataManager;
import org.junit.Before;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;

/**
 * Created by landon on 2/24/17.
 */
public abstract class DatatoolsTest {

    private static final Logger LOG = LoggerFactory.getLogger(DatatoolsTest.class);
    private static boolean setUpIsDone = false;

    @ClassRule
    public static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:9.4");

    @ClassRule
    public static GenericContainer mongo = new GenericContainer("mongo:3.4");

    @Before
    public void setUp() {
        if (setUpIsDone) {
            return;
        }

        //LOG.info("mongo mapped port {}", mongo.getMappedPort(27017));
        System.setProperty("MONGO_URI", "mongodb://"+mongo.getContainerIpAddress()+":"+mongo.getMappedPort(27017));
        System.setProperty("GTFS_DATABASE_URL", "jdbc:postgresql://"+postgres.getContainerIpAddress()+":"+postgres.getMappedPort(5432)+"/catalogue");


        LOG.info("DatatoolsTest setup");
        String[] args = {"configurations/test/env.yml.tmp", "configurations/test/server.yml.tmp"};
        try {
            DataManager.main(args);
            setUpIsDone = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

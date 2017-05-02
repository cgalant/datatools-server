package com.conveyal.datatools.manager.models;

import java.util.Date;

public class FeedDownloadS3Token extends Model {

    private static final long serialVersionUID = 1L;

    private Date timestamp;

    public FeedDownloadS3Token (String sessionToken) {
        super();
        id = sessionToken;
        timestamp = new Date();
    }

    public boolean isValid () {
        return true;
    }

    public void save () {
    }
}

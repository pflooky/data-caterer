package com.github.pflooky.datacatererutils;

import com.github.pflooky.datacatererutils.slack.TrialUtil;
import com.slack.api.bolt.App;
import com.slack.api.bolt.jetty.SlackAppServer;

public class DataCatererUtilsApplication {

    public static void main(String[] args) throws Exception {
        TrialUtil trialUtil = new TrialUtil();
        App app = new App();
        app.command("/token", (req, ctx) -> ctx.ack(trialUtil.generateApiKey(req, ctx)));

        SlackAppServer slackAppServer = new SlackAppServer(app, 8080);
        slackAppServer.start();
    }
}

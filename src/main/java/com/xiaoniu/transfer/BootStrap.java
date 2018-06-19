package com.xiaoniu.transfer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class BootStrap extends AbstractVerticle{
    @Override
    public void start(Future<Void> startFuture) throws Exception {
        vertx.deployVerticle(new MainVerticle(),new DeploymentOptions().setWorker(true).setMaxWorkerExecuteTime(120*1000));
    }
}

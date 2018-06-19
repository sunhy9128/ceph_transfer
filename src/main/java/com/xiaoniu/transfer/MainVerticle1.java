//package com.xiaoniu.transfer;//package com.xiaoniu.transfer;
//
//import com.amazonaws.services.s3.model.Bucket;
//import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
//import com.amazonaws.services.s3.model.CORSRule;
//import com.github.mauricio.async.db.mysql.MySQLConnection;
//import com.sun.org.apache.xpath.internal.SourceTree;
//import com.sun.org.apache.xpath.internal.VariableStack;
//
//import io.vertx.core.*;
//
//import io.vertx.core.file.FileSystem;
//import io.vertx.core.http.HttpServer;
//
//import io.vertx.core.json.JsonObject;
//import io.vertx.ext.asyncsql.AsyncSQLClient;
//import io.vertx.ext.asyncsql.MySQLClient;
//import io.vertx.ext.web.Router;
//import io.vertx.ext.web.handler.BodyHandler;
//import org.twonote.rgwadmin4j.RgwAdmin;
//import org.twonote.rgwadmin4j.RgwAdminBuilder;
//import org.twonote.rgwadmin4j.model.User;
//
//import java.net.URLEncoder;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.sql.Timestamp;
//import java.util.*;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//public class MainVerticle1 extends AbstractVerticle {
//    public static void main(String[] args) {
//        Vertx.vertx(new VertxOptions()).deployVerticle(new MainVerticle1(), new DeploymentOptions().setWorker(true).setMaxWorkerExecuteTime(5 * 60 * 1000));
//    }
//}

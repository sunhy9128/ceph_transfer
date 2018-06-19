package com.xiaoniu.transfer;


import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.CORSRule;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.twonote.rgwadmin4j.RgwAdmin;
import org.twonote.rgwadmin4j.RgwAdminBuilder;
import org.twonote.rgwadmin4j.model.User;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;


public class MainVerticle extends AbstractVerticle {

    @Override
    public void start() throws Exception {
        JsonObject config = new JsonObject(new String(Files.readAllBytes(Paths.get("config.json")), "UTF-8"));
        JsonObject dbConf = new JsonObject().put("host", config.getString("db.address"))
            .put("username", config.getString("db.username")).put("password", config.getString("db.password"))
            .put("database", config.getString("db.database")).put("port", 3306);


        String host = config.getString("aws.host");
        String host_admin = config.getString("aws.adminHost");
        String parentPath = config.getString("aws.parentPath");
        String jsPath=config.getString("sdk.js");
        String cPath=config.getString("sdk.c");
        Integer port = config.getInteger("project.port");
        HttpServer httpServer = vertx.createHttpServer();
        Router subRouter = Router.router(vertx);
        Router mainRouter = Router.router(vertx);
        BodyHandler bodyHandler = BodyHandler.create();
        subRouter.route().handler(bodyHandler);
        AsyncSQLClient asyncSQLClient = MySQLClient.createShared(vertx, dbConf);
        subRouter.post("/listBuckets").handler(ctx -> {
            JsonObject bodyAsJson = ctx.getBodyAsJson();
            String ak = bodyAsJson.getString("ak");
            String sk = bodyAsJson.getString("sk");
            CephApi cephApi = new CephApi(ak, sk, host);
            cephApi.setConn(ak,sk,host);
            List<Bucket> buckets = CephApi.client(ak,sk,host).listBuckets();
            buckets.stream().forEach(System.out::println);
            System.out.println("---------------------------------");
            JsonObject jsonObject = new JsonObject().put("buckets", buckets);
            ctx.response().putHeader("Access-Control-Allow-Origin", "*").end(jsonObject.encodePrettily());
        });
        subRouter.post("/bucketObjects").handler(ctx -> {
            JsonObject bodyAsJson = ctx.getBodyAsJson();
            RgwAdmin rgwAdmin = new RgwAdminBuilder().accessKey(bodyAsJson.getString("ak")).secretKey(bodyAsJson.getString("sk")).endpoint(host_admin).build();
            long bucketNum = rgwAdmin.getBucketInfo(bodyAsJson.getString("bucket")).get().getUsage().getRgwMain().getNum_objects();
            JsonObject jsonObject = new JsonObject().put("objectNum", bucketNum);
            ctx.response().putHeader("Access-Control-Allow-Origin", "*").end(jsonObject.encodePrettily());
        });
        subRouter.post("/createBucket").handler(ctx -> {
            JsonObject bodyAsJson = ctx.getBodyAsJson();
            CephApi cephApi = new CephApi(bodyAsJson.getString("ak"), bodyAsJson.getString("sk"), host);
            BucketCrossOriginConfiguration crossOriginConfiguration = new BucketCrossOriginConfiguration();
            try {
                Bucket bucket = CephApi.client(bodyAsJson.getString("ak"),bodyAsJson.getString("sk"),host).createBucket(bodyAsJson.getString("bucketName"));
                List<CORSRule> corsRules = new ArrayList<>();
                CORSRule corsRule = new CORSRule();
                corsRule.setAllowedOrigins("*");
                corsRule.setAllowedMethods(new CORSRule.AllowedMethods[]{CORSRule.AllowedMethods.DELETE, CORSRule.AllowedMethods.GET, CORSRule.AllowedMethods.HEAD, CORSRule.AllowedMethods.POST, CORSRule.AllowedMethods.PUT});
                corsRule.setAllowedHeaders("*");
                corsRules.add(corsRule);
                crossOriginConfiguration.setRules(corsRules);
                CephApi.client(bodyAsJson.getString("ak"),bodyAsJson.getString("sk"),host).setBucketCrossOriginConfiguration(bucket.getName(), crossOriginConfiguration);
                ctx.response().putHeader("Access-Control-Allow-Origin", "*").setStatusCode(200).end(new JsonObject().toString());
            } catch (Exception e) {
                e.printStackTrace();
                ctx.response().putHeader("Access-Control-Allow-Origin", "*").setStatusCode(404).end("error");
            }
        });
        subRouter.get("/zip").handler(ctx -> {
            CephApi cephApi = new CephApi(ctx.request().getParam("ak"), ctx.request().getParam("sk"), host);
            Map map = cephApi.downloadZipFile(ctx.request().getParam("bucket"), ctx.request().getParam("prefix"), parentPath);
            String s = map.get("filePath").toString();
            System.out.println(s);
            ctx.response().putHeader("Access-Control-Allow-Origin", "*").putHeader("Content-Disposition", "attachment;filename=" + URLEncoder.encode(map.get("fileName").toString().replaceAll("/", "") + ".zip")).sendFile(s, resultHandler -> {
                if (resultHandler.succeeded()) {
                    vertx.fileSystem().delete(s, del -> {
                        return;
                    });
                }
            });
        });
        subRouter.post("/register").handler(ctx -> {
            asyncSQLClient.query("SELECT * FROM bhfj.t_user where login='" + ctx.getBodyAsJson().getString("login") + "';", asyncResult -> {
//            asyncSQLClient.query("SELECT * FROM bhfj.t_user where login='"+ctx.getBodyAsJson().getString("login")+"';", asyncResult -> {
                if (asyncResult.succeeded()) {
                    System.out.println(asyncResult.result().toJson());
                    if (asyncResult.result().getResults().size() > 0) {
                        ctx.response().end(new JsonObject().put("type", "error").put("content", "用户名已存在").put("text", "用户名已存在").encodePrettily());
                    } else {
                        RgwAdmin rgwAdmin = new RgwAdminBuilder().accessKey(config.getString("aws.accessKey")).secretKey(config.getString("aws.secretAccessKey")).endpoint(config.getString("aws.adminHost")).build();
                        User user = rgwAdmin.createUser(ctx.getBodyAsJson().getString("login"));
                        asyncSQLClient.getConnection(handler -> {
                            UUID uuid = UUID.randomUUID();
                            handler.result().execute("INSERT INTO `bhfj`.`t_user` (`id`, `createTime`, `login`, `password`, `accessKey` , `secretKey`) VALUES ('" + uuid.toString().replaceAll("-", "") + "', '" + new Timestamp(System.currentTimeMillis()) + "', '" + ctx.getBodyAsJson().getString("login") + "', '" + ctx.getBodyAsJson().getString("password") + "', '" + user.getS3Credentials().get(0).getAccessKey() + "', '" + user.getS3Credentials().get(0).getSecretKey() + "');", ar -> {
                                if (ar.succeeded()) {
                                    JsonObject retData = new JsonObject();
                                    retData.put("access_key", user.getS3Credentials().get(0).getAccessKey());
                                    retData.put("id", uuid.toString().replaceAll("-", ""));
                                    retData.put("secret_key", user.getS3Credentials().get(0).getSecretKey());
                                    JsonObject appMessage = new JsonObject().put("type", "success").put("content", "OK").put("test", "OK").put("code", "0").put("data", retData);
                                    ctx.response().putHeader("Access-Control-Allow-Origin", "*").end(appMessage.encodePrettily());
                                }
                            });
                        });
                    }

                } else {
                    asyncResult.cause().printStackTrace();
                    ctx.response().putHeader("Access-Control-Allow-Origin", "*").end(new JsonObject().put("type", "error").put("content", "connect error").put("text", "connect error").encodePrettily());
                }
            });
        });
        subRouter.post("/login").handler(ctx -> {
            JsonObject bodyAsJson = ctx.getBodyAsJson();
            String login = bodyAsJson.getString("phone");
            String password = bodyAsJson.getString("password");
            asyncSQLClient.query("SELECT * FROM bhfj.t_user where login='" + login + "';", asyncResult -> {
//            asyncSQLClient.query("SELECT * FROM bhfj.t_user where login='"+login+"';",asyncResult->{
                if (asyncResult.succeeded()) {
                    if (asyncResult.result().getResults().size() == 0) {
                        ctx.response().putHeader("Access-Control-Allow-Origin", "*").end(new JsonObject().put("type", "error").put("content", "用户名不存在").put("text", "用户名不存在").encodePrettily());
                    } else {
                        JsonObject object = asyncResult.result().getRows().get(0);
                        String password1 = object.getString("password");
                        if (password1.equals(password)) {
                            JsonObject retData = new JsonObject();
                            retData.put("name", object.getString("login"));
                            retData.put("id", object.getString("id"));
                            retData.put("accessKey", object.getString("accessKey"));
                            retData.put("secretKey", object.getString("secretKey"));
//                            retData.put("isAdmin",object.getInteger("isAdmin")==1?"yes":"no");
                            JsonObject appMessage = new JsonObject().put("type", "success").put("content", "OK").put("test", "OK").put("code", "0").put("data", retData);
                            ctx.response().putHeader("Access-Control-Allow-Origin", "*").end(appMessage.encodePrettily());
                        } else {
                            ctx.response().putHeader("Access-Control-Allow-Origin", "*").setStatusCode(404).end(new JsonObject().put("type", "error").put("content", "密码错误").put("text", "密码错误").encodePrettily());
                        }
                    }
                } else {
                    asyncResult.cause().printStackTrace();
                    ctx.response().putHeader("Access-Control-Allow-Origin", "*").setStatusCode(404).end(new JsonObject().put("type", "error").put("content", "error").put("text", "error").encodePrettily());
                }
            });
        });
        subRouter.get("/javascript").handler(ctx->{
            ctx.response().putHeader("Access-Control-Allow-Origin", "*").putHeader("Content-Disposition", "attachment;filename=" + URLEncoder.encode("Ceph-SDK-1.0.0.js")).sendFile(jsPath, resultHandler -> {
                if (resultHandler.succeeded()) {
                   return;
                }
            });
        });
        subRouter.get("/c").handler(ctx->{
            System.out.println("here");
            ctx.response().putHeader("Access-Control-Allow-Origin", "*").putHeader("Content-Disposition", "attachment;filename=" + URLEncoder.encode("Ceph-C-SDK-1.0.zip")).sendFile(cPath, resultHandler -> {
                if (resultHandler.succeeded()) {
                    return;
                }
            });
        });
        mainRouter.mountSubRouter("/api", subRouter);
        httpServer.requestHandler(mainRouter::accept).listen(port);
    }

}

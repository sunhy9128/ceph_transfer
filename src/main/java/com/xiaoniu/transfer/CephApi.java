package com.xiaoniu.transfer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringUtils;


import java.io.*;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


public class CephApi {


    private static AmazonS3 conn = null;
    private String accessKey = null;
    private String secretKey = null;
    private String serverHost = null;
    private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public static AWSCredentials awsCredentials;

    public CephApi() {
    }

    public void shutdown() {
        conn.shutdown();
    }

    public CephApi(String accessKey, String secretKey, String host) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.serverHost=host;
        this.setConn(accessKey, secretKey, host);
    }


    public void setConn(String access_key, String secret_key, String host) {
        if (awsCredentials == null) {
            awsCredentials = new BasicAWSCredentials(access_key, secret_key);
            ClientConfiguration clientConfig = new ClientConfiguration();
            clientConfig.setProtocol(Protocol.HTTP);
            AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withClientConfiguration(clientConfig)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(host, Region.US_West_2.toAWSRegion().getName()));
            conn = amazonS3ClientBuilder.build();
        }
    }

    public AmazonS3 getConn() {
        return conn;
    }

    public List<Bucket> getBuckets() {
        List<Bucket> buckets = conn.listBuckets();
        return buckets;
    }

    public Bucket getBucketsByName(String bucketName) {
        Bucket resultBucket = null;
        if (bucketName.isEmpty()) {
            return null;
        }
        List<Bucket> buckets = conn.listBuckets();
        if (buckets == null) {
            return resultBucket;
        }
        for (Bucket bucket : buckets) {
            if (bucketName.equals(bucket.getName())) {
                resultBucket = bucket;
                break;
            }
        }
        return resultBucket;
    }

    /**
     * 新建容器名称
     *
     * @param bucketName
     * @return
     */
    public Bucket createBucket(String bucketName) {
        if (bucketName.isEmpty()) {
            return null;
        }
        Bucket bucket = conn.createBucket(bucketName);

        return bucket;
    }

    /**
     * 获取该容器下面的所有信息（文件目录集合和文件信息集合）
     *
     * @param bucketName
     * @return
     */
    public ObjectListing getBucketObjects(String bucketName) {
        if (bucketName.isEmpty()) {
            return null;
        }
        ObjectListing objects = conn.listObjects(bucketName);
        return objects;
    }

    public ObjectListing getBucketObjects(Bucket bucket) {
        if (bucket.getName().isEmpty()) {
            return null;
        }
        ObjectListing objects = conn.listObjects(bucket.getName());
        return objects;
    }

    /**
     * 获取某个文件（前缀路径）下的所有信息
     *
     * @param bucketName
     * @param prefix
     * @param isDelimiter
     * @return
     */
    public ObjectListing getBucketObjects(String bucketName, String prefix, Boolean isDelimiter) {
        if (bucketName == null || bucketName.isEmpty()) {
            return null;
        }
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
        if (prefix != null && !prefix.isEmpty()) {
            listObjectsRequest = listObjectsRequest.withPrefix(prefix);
        }
        if (isDelimiter) {
            listObjectsRequest = listObjectsRequest.withDelimiter("/");
        }
        ObjectListing objectListing = conn.listObjects(listObjectsRequest);
        return objectListing;
    }

    public ObjectListing getBucketObjects(Bucket bucket, String prefix, Boolean isDelimiter) {
        if (bucket.getName() == null || bucket.getName().isEmpty()) {
            return null;
        }
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucket.getName());
        if (prefix != null && !prefix.isEmpty()) {
            listObjectsRequest = listObjectsRequest.withPrefix(prefix);
        }
        if (isDelimiter) {
            listObjectsRequest = listObjectsRequest.withDelimiter("/");
        }
        ObjectListing objectListing = conn.listObjects(listObjectsRequest);
        return objectListing;
    }

    public ObjectListing getBucketObjects(String bucketName, String prefix) {
        return this.getBucketObjects(bucketName, prefix, false);
    }

    public ObjectListing getBucketObjects(Bucket bucket, String prefix) {
        return this.getBucketObjects(bucket.getName(), prefix, true);
    }

    /**
     * 获取当前容器下面的目录集合
     *
     * @return
     */
//    public List<StorageObjectVO> getDirectList(ObjectListing objects) {
//        List<StorageObjectVO> directList = new ArrayList<StorageObjectVO>();
//        String prefix = objects.getPrefix();
//        do {
//            List<String> commonPrefixes = objects.getCommonPrefixes();
//            for (String commonPrefix : commonPrefixes) {
//                StorageObjectVO dirStorageObjectVO = new StorageObjectVO();
//                String dirPath = commonPrefix.substring(prefix == null ? 0 : prefix.length(), commonPrefix.length() - 1);
//                dirStorageObjectVO.setName(dirPath);
//                dirStorageObjectVO.setType("documentPath");
//                directList.add(dirStorageObjectVO);
//
//            }
//            objects = conn.listNextBatchOfObjects(objects);
//        } while (objects.isTruncated());
//        return directList;
//    }
    public AccessControlList getBucketAcl(String bucketName) {
        return conn.getBucketAcl(bucketName);
    }

    public AccessControlList getBucketAcl(Bucket bucket) {
        return conn.getBucketAcl(bucket.getName());
    }

    public AccessControlList getBucketAcl(GetBucketAclRequest request) {
        return conn.getBucketAcl(request);
    }

    public void setBucketAcl(String bucketName, AccessControlList accessControlList) {
        conn.setBucketAcl(bucketName, accessControlList);
    }

    /**
     * 获取当前容器下面的文件集合
     *
     * @return
     */
//    public List<StorageObjectVO> getFileList(ObjectListing objects) {
//        List<StorageObjectVO> fileList = new ArrayList<StorageObjectVO>();
//        String prefix = objects.getPrefix();
//        do {
//            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
//                System.out.println(objectSummary.getKey() + "\t" + objectSummary.getSize() + "\t" + StringUtils.fromDate(objectSummary.getLastModified()));
//                if (prefix != null && objectSummary.getKey().equals(prefix.trim())) {
//                    continue;
//                }
//                StorageObjectVO fileStorageObjectVO = new StorageObjectVO();
//                String fileName = objectSummary.getKey().substring(prefix == null ? 0 : prefix.length(), objectSummary.getKey().length());
//                fileStorageObjectVO.setName(objectSummary.getKey());
//                fileStorageObjectVO.setName(fileName);
//                fileStorageObjectVO.setType("document");
//                fileStorageObjectVO.setSize(bytes2kb(objectSummary.getSize()));
//                fileStorageObjectVO.setDate(df.format(objectSummary.getLastModified()));
//                fileList.add(fileStorageObjectVO);
//            }
//            objects = conn.listNextBatchOfObjects(objects);
//        } while (objects.isTruncated());
//        return fileList;
//    }

//    public List<StorageObjectVO> getObjectList(String bucketName, String prefix) {
//        if (bucketName == null && bucketName.isEmpty()) {
//            return null;
//        }
//        ListObjectsRequest objectsRequest = new ListObjectsRequest().withBucketName(bucketName);
//        if (prefix != null && !prefix.isEmpty()) {
//            objectsRequest = objectsRequest.withPrefix(prefix);
//        }
//        objectsRequest = objectsRequest.withDelimiter("/");
//        ObjectListing objects = conn.listObjects(objectsRequest);
//        List<StorageObjectVO> resultList = new ArrayList<StorageObjectVO>();
//        List<StorageObjectVO> dirList = getDirectList(objects);
//        if (dirList != null && dirList.size() > 0) {
//            resultList.addAll(dirList);
//        }
//        List<StorageObjectVO> fileList = getFileList(objects);
//        if (fileList != null && fileList.size() > 0) {
//            resultList.addAll(fileList);
//        }
//
//        return resultList;
//    }
//
//    public List<StorageObjectVO> getObjectList(Bucket bucket, String prefix) {
//        if (bucket.getName() == null && bucket.getName().isEmpty()) {
//            return null;
//        }
//        ListObjectsRequest objectsRequest = new ListObjectsRequest().withBucketName(bucket.getName());
//        if (prefix != null && !prefix.isEmpty()) {
//            objectsRequest = objectsRequest.withPrefix(prefix);
//        }
//        objectsRequest = objectsRequest.withDelimiter("/");
//        ObjectListing objects = conn.listObjects(objectsRequest);
//        List<StorageObjectVO> resultList = new ArrayList<StorageObjectVO>();
//        List<StorageObjectVO> dirList = getDirectList(objects);
//        if (dirList != null && dirList.size() > 0) {
//            resultList.addAll(dirList);
//        }
//        List<StorageObjectVO> fileList = getFileList(objects);
//        if (fileList != null && fileList.size() > 0) {
//            resultList.addAll(fileList);
//        }
//
//        return resultList;
//    }

    //    //创建文件目录
    public Boolean createPath(String bucketName, String StorageObjectVOPath, String folderName) {
        String key = "";
        if (bucketName == null || folderName == null) {
            return false;
        }
        if (StorageObjectVOPath == null || StorageObjectVOPath.isEmpty() || "null".equals(StorageObjectVOPath)) {
            StorageObjectVOPath = "";
            key = "" + folderName + "/";
        } else {
            key = StorageObjectVOPath + "/" + folderName + "/";
        }
        ByteArrayInputStream local = new ByteArrayInputStream("".getBytes());
        PutObjectResult result = conn.putObject(bucketName, key, local, new ObjectMetadata());
        return true;

    }

    public Boolean deleteBucket(String bucketName) {
        if (bucketName.isEmpty()) {
            return false;
        }
        Bucket bucket = conn.createBucket(bucketName);
        conn.deleteBucket(bucket.getName());
        return true;
    }

    /**
     * 上传 文件对象到容器
     *
     * @param bucketName
     * @param StorageObjectVOPath
     * @param fileName
     * @param uploadFile
     * @return
     */
    public PutObjectResult createObject(String bucketName, String StorageObjectVOPath, String fileName, File uploadFile) {
        if (StorageObjectVOPath == null || StorageObjectVOPath.isEmpty() || "null".equals(StorageObjectVOPath)) {
            StorageObjectVOPath = "";
        }
        if (uploadFile == null) {
            return null;
        }
        String fileAllPath = StorageObjectVOPath + fileName;
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(uploadFile);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        PutObjectResult result = conn.putObject(bucketName, fileAllPath, inputStream, new ObjectMetadata());
        return result;
    }


    public void changeObjectACL(String bucketName, String ObjectName, CannedAccessControlList access) {

        conn.setObjectAcl(bucketName, ObjectName, access);

    }


    public ObjectMetadata download(String bucketName, String objectName, File destinationFile) {
        if (bucketName.isEmpty() || objectName.isEmpty()) {
            return null;
        }
        ObjectMetadata result = conn.getObject(new GetObjectRequest(bucketName, objectName), destinationFile);
        return result;
    }

    public S3Object download(String bucketName, String objectName) {
        if (bucketName.isEmpty() || objectName.isEmpty()) {
            return null;
        }
        S3Object object = conn.getObject(bucketName, objectName);
        return object;
    }

    public Boolean deleteObject(String bucketName, String objectName) {
        if (bucketName.isEmpty() || objectName.isEmpty()) {
            return false;
        }
        conn.deleteObject(bucketName, objectName);
        return true;
    }

    /**
     * 生成文件url
     *
     * @param bucketName
     * @param objectName
     * @return
     */
    public String getDownloadUrl(String bucketName, String objectName) {
        if (bucketName.isEmpty() || objectName.isEmpty()) {
            return null;
        }
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, objectName);
        URL url = conn.generatePresignedUrl(request);
        return url.toString();
    }

    /**
     * 移动对象信息到目标容器
     *
     * @param OrgbucketName
     * @param orgKey
     * @param destinationName
     * @param destinationKey
     * @return
     */
    public Boolean moveObject(String OrgbucketName, String orgKey, String destinationName, String destinationKey) {
        CopyObjectResult result = conn.copyObject(OrgbucketName, orgKey, destinationName, destinationKey);
        Boolean isDelete = deleteObject(OrgbucketName, orgKey);
        if (result != null) {
            return isDelete;
        }
        return false;
    }

    /**
     * 移动目标文件夹信息到目标容器
     *
     * @param objects
     * @param destinationBucket
     * @return
     */
    public Boolean moveFolder(ObjectListing objects, String destinationBucket) {
        String bucketName = objects.getBucketName();
        do {

            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                System.out.println(objectSummary.getKey() + "\t" + objectSummary.getSize() + "\t" + StringUtils.fromDate(objectSummary.getLastModified()));
                CopyObjectResult result = conn.copyObject(bucketName, objectSummary.getKey(), destinationBucket, objectSummary.getKey());
                Boolean isDelete = deleteObject(bucketName, objectSummary.getKey());
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
        return true;
    }

    /**
     * 删除文件夹内容（必须先遍历删除文件夹内的内容）
     *
     * @param objects
     * @return
     */
    public Boolean deleteFolder(ObjectListing objects) {
        String bucketName = objects.getBucketName();
        do {

            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                System.out.println(objectSummary.getKey() + "\t" + objectSummary.getSize() + "\t" + StringUtils.fromDate(objectSummary.getLastModified()));
                Boolean isDelete = deleteObject(bucketName, objectSummary.getKey());
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
        return true;
    }

    public Map downloadZipFile(String bucketName, String prefix, String parentPath) {
        try {
            byte[] buffer = new byte[1024];
            String parentFolderPath = "";
            parentFolderPath = parentPath.endsWith(File.separator) ? parentPath : parentPath + File.separator;
            String nPrefix = prefix.endsWith(File.separator) ? prefix : prefix + File.separator;
            String folder;
            String finalFile;
            if (!nPrefix.equals("/")) {
                folder = nPrefix.substring(nPrefix.indexOf(nPrefix.split(File.separator)[nPrefix.split(File.separator).length - 1]));
                finalFile = parentFolderPath + folder.replaceAll(File.separator, "") + ".zip";

            } else {
                folder = "";
                finalFile = parentFolderPath + "ceph" + System.currentTimeMillis() + ".zip";
            }
            System.out.println(finalFile);
            ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(finalFile));
            List<S3ObjectSummary> objectSummaries = client(accessKey,secretKey,serverHost).listObjects(bucketName, prefix).getObjectSummaries();

            for (S3ObjectSummary objectSummary : objectSummaries) {
                String key = objectSummary.getKey();
                if (!key.endsWith(File.separator)) {
                    S3ObjectInputStream s3ObjectInputStream = client(accessKey,secretKey,serverHost).getObject(bucketName, objectSummary.getKey()).getObjectContent();
                    String fileName = key.substring(key.indexOf(folder));
                    zipOutputStream.putNextEntry(new ZipEntry(fileName));
                    int len;
                    while ((len = s3ObjectInputStream.read(buffer)) != -1) {
                        zipOutputStream.write(buffer, 0, len);

                    }
                    s3ObjectInputStream.close();
                    zipOutputStream.closeEntry();
                }
            }
            zipOutputStream.close();
            HashMap<String, String> map = new HashMap<>();
            map.put("filePath", finalFile);
            map.put("fileName", folder);
            return map;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static AmazonS3 client(String ak,String sk,String host){
        AWSCredentials awsCredentials = new BasicAWSCredentials(ak,sk);
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);
        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
            .withClientConfiguration(clientConfig)
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(host, Region.US_West_2.toAWSRegion().getName()));
        AmazonS3 amazonS3 = amazonS3ClientBuilder.build();
        return amazonS3;
    }
    /**
     * 将文件大小格式转为MB格式
     *
     * @param bytes
     * @return
     */
    public static String bytes2kb(long bytes) {
        BigDecimal fileSize = new BigDecimal(bytes);
        BigDecimal megabyte = new BigDecimal(1024 * 1024);
        float returnValue = fileSize.divide(megabyte, 2, BigDecimal.ROUND_UP)
            .floatValue();
        if (returnValue > 1)
            return (returnValue + "MB");
        BigDecimal kilobyte = new BigDecimal(1024);
        returnValue = fileSize.divide(kilobyte, 2, BigDecimal.ROUND_UP)
            .floatValue();
        return (returnValue + "KB");
    }

    private static boolean deleteDir(String dir) {
        File dirFile = new File(dir);
        if (dirFile.isDirectory()) {
            String[] children = dirFile.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(dir + "/" + children[i]);
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dirFile.delete();
    }
}

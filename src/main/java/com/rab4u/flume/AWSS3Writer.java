package com.rab4u.flume;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import java.io.*;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class AWSS3Writer {

    private AmazonS3 s3Client;

    AWSS3Writer(String clientRegion) {

        try {
            s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }

    public Boolean putFile(String bucketName, String filePrefix, String fileSufix,String file) throws IOException {

        try{

            //GENERATE TIMEBASED UUID
            NoArgGenerator timeBasedGenerator = Generators.timeBasedGenerator();
            UUID firstUUID = timeBasedGenerator.generate();

            // DATE AND TIME FOR FILE PATH
            OffsetDateTime utc = OffsetDateTime.now(ZoneOffset.UTC);
            Date date = Date.from(utc.toInstant());
            SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat hours_format = new SimpleDateFormat("HH");
            String dateString = date_format.format(date);
            String hours = hours_format.format(date);

            PutObjectRequest request = new PutObjectRequest(bucketName + "/partition-date=" + dateString + "/partition-hours=" + hours + "-00", filePrefix + "-" + firstUUID.toString() + "." + fileSufix, new File(file));
            ObjectMetadata metadata = new ObjectMetadata();
            FileInputStream fin=new FileInputStream(file);
            String mimeType = URLConnection.guessContentTypeFromStream(fin);
            metadata.setContentType(mimeType);
            metadata.setContentLength(fin.available());
            request.setMetadata(metadata);
            s3Client.putObject(request);

            System.out.println("[INFO] DATA SUCCESSFULLY WRITEN TO S3 PATH : S3://" + bucketName + "/partition-date=" + dateString + "/partition-hours" + hours + "-00" + "/" + filePrefix + "-" + firstUUID.toString() + "." + fileSufix);

            return true;

        }
        catch (Exception e){
            e.printStackTrace();
            throw e;
        }

    }

    public static void main(String args[]){

        String clientRegion = "eu-central-1";
        String bucketName = "some bucket name";
        String filePrefix = "bala bala";
        String fileSufix = "avro";
        String file = "/home/ravindrachellubani/Documents/code/git/flumes3sink/src/main/java/com/rab4u/flume/test_file.avro";

        AWSS3Writer as3w = new AWSS3Writer(clientRegion);

        try {
            as3w.putFile(bucketName, filePrefix, fileSufix, file);
        }
        catch (Exception e){}

    }
}

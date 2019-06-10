package com.rab4u.flume;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.RollingFileSink;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class FlumeS3Sink extends AbstractSink implements Configurable, BatchSizeSupported {

    // LOGGING
    private static final Logger logger = LoggerFactory
            .getLogger(RollingFileSink.class);

    // SINK DEFAULT PARAMETERS
    private static final long defaultRollInterval = 300;
    private static final int defaultBatchSize = 500;
    private String defaultAvroSchema = "";
    private String defaultAvroSchemaRegistryURL = "";
    private String defaultFilePrefix = "flumeS3Sink";
    private String defaultFileSufix = ".data";
    private String defaultTempFile = "/tmp/flumes3sink/data/file";
    private String defaultCompress = "false";

    // SINK CONF PARAMETERS
    private int batchSize = defaultBatchSize;
    private long rollInterval = defaultRollInterval;
    private String avroSchema = defaultAvroSchema;
    private String avroSchemaRegistryURL = defaultAvroSchemaRegistryURL;
    private String filePrefix = defaultFilePrefix;
    private String fileSufix = defaultFileSufix;
    private String bucketName;
    private String awsRegion;
    private String tempFile = defaultTempFile;
    private String compress = defaultCompress;


    // SINK COUNTER
    private SinkCounter sinkCounter;

    private Boolean shouldRotate;
    private ScheduledExecutorService rollService;

    //STREAMS
    private DataFileWriter<GenericRecord> dataFileWriter = null;

    // AWS
    private AWSS3Writer as3w;

    // SINK COUNTER
    private long sink_current_counter = 0;

    //DERSERIALIZE CONSTANTS
    private static final byte MAGIC_BYTE = 0x0;
    private static final int idSize = 4;

    //FILE OUTPUT STREAM
    private FileOutputStream fop = null;

    // Start transaction
    private Status status = null;
    private Channel ch = null;
    private Transaction txn = null;

    // SCHEMA
    private String schemaStr = null;
    private Schema schema = null;

    // WRITER
    private DatumWriter<GenericRecord> datumWriter = null;

    private long tempFileSize;
    private File currentTempFile;



    public FlumeS3Sink() {
        shouldRotate = false;
    }

    public void configure(Context context) {

        // Reading the parameters from flume conf
        int batchSize = context.getInteger("s3.batchSize", defaultBatchSize);
        long rollInterval = context.getLong("s3.rollInterval", defaultRollInterval);
        String avroSchema = context.getString("s3.AvroSchema", defaultAvroSchema);
        String avroSchemaRegistryURL = context.getString("s3.AvroSchemaRegistryURL", defaultAvroSchemaRegistryURL);
        String filePrefix = context.getString("s3.filePrefix", defaultFilePrefix);
        String fileSufix = context.getString("s3.FileSufix", defaultFileSufix);
        String bucketName = context.getString("s3.bucketName");
        String awsRegion = context.getString("s3.awsRegion");
        String tempFile = context.getString("s3.tempFile", defaultTempFile);
        String compress = context.getString("s3.compress", defaultCompress);

        // VALIDATIONS FOR REQUIRED PARAMETERS IN FLUME CONF
        Preconditions.checkArgument(bucketName != null, "[INFO] BUCKET NAME IS NOT SPECIFIED (example :  s3.bucket = \"aws_bucket_name\")");
        Preconditions.checkArgument(awsRegion != null, "[INFO] AWS REGION IS NOT SPECIFIED (example :  s3.awsRegion = \"eu-central-1\")");

        // Store myProp for later retrieval by process() method
        this.batchSize = batchSize;
        this.rollInterval = rollInterval;
        this.avroSchema = avroSchema;
        this.avroSchemaRegistryURL = avroSchemaRegistryURL + "/versions/latest";
        this.filePrefix = filePrefix;
        this.fileSufix = fileSufix;
        this.bucketName = bucketName;
        this.awsRegion = awsRegion;
        this.sink_current_counter = batchSize;
        this.tempFile = tempFile;
        this.compress = compress;

        //GET SINK COUNTER
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }

    }

    @Override
    public void start() {

        logger.info("STARTING AGENT {} ...", this);

        //CREATE A TEMP FILE PATH TO PARTIALLY HOLD DATA BEFORE SENDING TO AWS
        double randomDouble = Math.random();
        randomDouble = randomDouble * 1500 + 1;
        int randomInt = (int) randomDouble;
        this.tempFile = this.tempFile + "-" + randomInt + "." + this.fileSufix;
        logger.info("SUCCESSFULLY CREATED TEMP FILE PATH : {}", this.tempFile);

        // OPEN THE FILE
        currentTempFile = new File(tempFile);

        // Initialize the connection to the external repository such as AWS OR HDFS
        as3w = new AWSS3Writer(this.awsRegion);
        logger.info("AWS S3 WRITER INITIALIZED SUCCESSFULLY : {}", as3w);

        // STARTING THE SINK COUNTER
        sinkCounter.start();

        // FILE ROTATION
        super.start();
        if (rollInterval > 0) {

            rollService = Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder().setNameFormat(
                            "rollingFileSink-roller-" +
                                    Thread.currentThread().getId() + "-%d").build());
            rollService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    logger.info("MARKING THE TIME TO ROLL INTERVAL {}", rollInterval);
                    shouldRotate = true;
                }

            }, rollInterval, rollInterval, TimeUnit.SECONDS);
        } else {
            logger.info("ROLLINTERVAL IS NOT VALID, FILE ROLLING WILL NOT HAPPEN.");
        }

        // GET THE SCHEMA
        if ((this.avroSchema != null && !this.avroSchema.isEmpty()) || (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty())) {

            if (this.avroSchema != null && !this.avroSchema.isEmpty()) {
                schemaStr = getSchemaFromPath(this.avroSchema);
            } else if (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty()) {
                schemaStr = getSchemaFromURL(this.avroSchemaRegistryURL);
            }

            schema = getAvroSchema(schemaStr);

        }


        logger.info("FLUME S3 SINK {} started...", getName());

    }

    @Override
    public void stop() {
        // Disconnect from the external respository and do any
        // additional cleanup (e.g. releasing resources or nulling-out
        // field values) ..
    }

    public Status process() throws EventDeliveryException {

        // PRINT PARAMS
        logger.info("BUCKET NAME         : {}", this.bucketName);
        logger.info("AWS REGION          : {}", this.awsRegion);
        logger.info("FILE PREFIX         : {}", this.filePrefix);
        logger.info("FILE SUFIX          : {}", this.fileSufix);
        logger.info("FILE ROLL INTERVAL  : {}", this.rollInterval);
        logger.info("TEMP FILE           : {}", this.tempFile);
        logger.info("AVRO SCHEMA         : {}", this.avroSchema);
        logger.info("BATCH SIZE          : {}", this.batchSize);
        logger.info("SINK COUNTER        : {}", this.sink_current_counter);
        logger.info("SHOULD ROTATE       : {}", this.shouldRotate);
        logger.info("COMPRESS            : {}", this.compress);

        // GET THE SCHEMA
        if ((this.avroSchema != null && !this.avroSchema.isEmpty()) || (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty())) {

            if (this.avroSchema != null && !this.avroSchema.isEmpty()) {
                schemaStr = getSchemaFromPath(this.avroSchema);
            } else if (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty()) {
                schemaStr = getSchemaFromURL(this.avroSchemaRegistryURL);
            }

            Schema current_schema = getAvroSchema(schemaStr);
            if (!current_schema.toString().equalsIgnoreCase(schema.toString())){
                logger.info("[INFO] SCHEMA IS CHANGED. HENCE FILE IS ROTATING...");
                System.out.println("[INFO] SCHEMA IS CHANGED. HENCE FILE IS ROTATING...");
                shouldRotate = true;
            }

            schema = current_schema;

        }

        if (shouldRotate || sink_current_counter == this.batchSize) {
            logger.info("TIME TO ROLL {} THE FILE TO S3", this.tempFile);

            if (!currentTempFile.exists()) {    // RUNS ONLY ONCE WHEN STARTING THE SINK
                try {
                    currentTempFile.createNewFile();
                    fop = new FileOutputStream(currentTempFile);

                    if ((this.avroSchema != null && !this.avroSchema.isEmpty()) || (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty())) {
                        // GET WRITER
                        datumWriter = new GenericDatumWriter<GenericRecord>(schema);
                        dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
                        if(this.compress.equals("true")) {
                            dataFileWriter.setCodec(CodecFactory.snappyCodec());
                        }
                        dataFileWriter.create(schema, fop);
                        tempFileSize = currentTempFile.length();
                    }


                    sink_current_counter = 0;
                    shouldRotate = false;

                    ch = getChannel();
                    txn = ch.getTransaction();
                    txn.begin();

                } catch (IOException e) {
                    logger.info("CANNOT CREATE TEMP FILE TO PARTIALLY STORE AWS DATA {}:", this.tempFile);
                    e.printStackTrace();
                    throw new EventDeliveryException("Failed to process transaction", e);
                }
            } else {

                // CHECK IF THE FILE IS EMPTY
//                System.out.println("FILE NAME : "+currentTempFile.getName());
//                System.out.println("FILE SIZE : "+currentTempFile.length());
//                System.out.println("FILE LASTMODIFIED : "+currentTempFile.lastModified());

                if (currentTempFile.length() == 0 || currentTempFile.length() == tempFileSize) {

                    logger.info("TEMP FILE IS EMPTY SO NO DATA TO WRITE TO S3 {}:", this.tempFile);
                    System.out.println("[INFO] TOTAL NO. OF MESSAGES WRITTEN TO S3 TILL NOW : " + sinkCounter.getEventDrainSuccessCount());
                    System.out.println("[INFO] NO DATA TO WRITE TO S3 ");
                    sink_current_counter = 0;
                    shouldRotate = false;
                }
                else {
                    // AWS WRITE DATA
                    try {

                        as3w.putFile(this.bucketName, this.filePrefix, this.fileSufix, this.tempFile);

                        currentTempFile.delete();
                        currentTempFile = new File(tempFile);
                        currentTempFile.createNewFile();
                        fop = new FileOutputStream(currentTempFile);

                        // GET WRITER
                        if ((this.avroSchema != null && !this.avroSchema.isEmpty()) || (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty())) {
                            datumWriter = new GenericDatumWriter<GenericRecord>(schema);
                            dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
                            if(this.compress.equals("true")) {
                                dataFileWriter.setCodec(CodecFactory.snappyCodec());
                            }
                            dataFileWriter.create(schema, fop);
                        }

                        sink_current_counter = 0;
                        shouldRotate = false;

                        logger.info("WRITING DATA FORM TEMP FILE {} TO S3 {} SUCCESSFUL", this.tempFile, this.bucketName);
                        logger.info("TOTAL NO. OF MESSAGES WRITTEN TO S3 {}: ",sinkCounter.getEventDrainSuccessCount());
                        System.out.println("[INFO] TOTAL NO. OF MESSAGES WRITTEN TO S3 TILL NOW : " + sinkCounter.getEventDrainSuccessCount());
                        txn.commit();
                    } catch (IOException e) {
                        txn.rollback();
                        status = Status.BACKOFF;
                        logger.info("WRITING DATA FORM TEMP FILE {} TO S3 {} FAILED",this.tempFile,this.bucketName);
                        e.printStackTrace();
                        throw new EventDeliveryException("Failed to process transaction", e);
                    } finally {
                        txn.close();
                        txn = null;
                    }
                }

                // GET TRANSACTION IF TXN IS NULL
                if (txn ==  null){
                    ch = getChannel();
                    txn = ch.getTransaction();
                    txn.begin();
                }
            }

        }
        // WRITE THE DATA TEMP FILE
        try {

            Event event = ch.take();
            if (event != null) {

             //   System.out.println(event.getHeaders());

                byte[] serializeBytes = event.getBody();
                Map<String, String> eventHeaders = event.getHeaders();

                if ((this.avroSchema != null && !this.avroSchema.isEmpty()) || (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty())) {
                    // AVRO BINARY FORMAT WITHOUT SCHEMA
                    ByteArrayDeserializer bad = new ByteArrayDeserializer();
                    byte[] deserializeBytes = bad.deserialize(eventHeaders.get("topic"), serializeBytes);

                    ByteBuffer buffer = getByteBuffer(deserializeBytes);
                    int id = buffer.getInt();
                    int length = buffer.limit() - 1 - idSize;
                    final Object result;
                    int start = buffer.position() + buffer.arrayOffset();
                    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                    final DecoderFactory decoderFactory = DecoderFactory.get();

                    GenericRecord userData = null;
                    userData = reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));

                    dataFileWriter.append(userData);
                    dataFileWriter.flush();

                    sinkCounter.incrementEventDrainSuccessCount();
                    long success_count = sinkCounter.getEventDrainSuccessCount();
                    if(success_count <= batchSize)
                        sink_current_counter = success_count;
                    else
                        sink_current_counter++;

//                    System.out.println("[INFO] SUCCESSFULLY WRITTEN MESSAGE. TOTAL NO. OF MESSAGES PROCESSED : " + sink_current_counter);
//                    System.out.println("MESSAGE CONTENT : " + userData.toString());

                } else {
                    // STRING FORMAT
                    String s = new String(serializeBytes);
                    byte b[] = s.getBytes();
                    fop.write(b);

                    sinkCounter.incrementEventDrainSuccessCount();
                    long success_count = sinkCounter.getEventDrainSuccessCount();
                    if(success_count <= batchSize)
                        sink_current_counter = success_count;
                    else
                        sink_current_counter++;

                }
            }

            status = Status.READY;

        } catch (Exception e) {
            txn.rollback();
            e.printStackTrace();
            status = Status.BACKOFF;
            logger.info("FAILED TO WRITE DATA TO TEMP FILE {}", this.tempFile);
            throw new EventDeliveryException("Failed to process transaction", e);
        }
        return status;
    }

    public String getSchemaFromURL(String schemaRegistryURL) {
        try {

            URL schemaData = new URL(schemaRegistryURL);
            String schemaJsonData = Resources.toString(schemaData, Charsets.UTF_8).replace("\\\"", "\"")
                    .replace("\"{", "{").replace("}\"", "}");
            JSONObject obj = new JSONObject(schemaJsonData);
            String data = obj.getJSONObject("schema")
                    .toString() ;
//                    .replace("\"string\"", "{\"type\": \"string\",\"avro.java.string\": \"String\"}");

            return data;

        } catch (MalformedURLException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return "";
    }

    public String getSchemaFromPath(String schemaPath) {

        try {

            Path file_path = Paths.get(schemaPath);
            String data = new String(Files.readAllBytes(file_path), "UTF-8");

            return data;
        } catch (UnsupportedEncodingException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return "";
    }

    public Schema getAvroSchema(String data) {

        if (data != null && !data.isEmpty()) {
            try {
                Schema schema = new Schema.Parser().parse(data);

                return schema;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return null;
    }

    private ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte!");
        }
        return buffer;
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

}
package io.oferto.pocminios3select.service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Service;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointParams;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;
import software.amazon.awssdk.services.s3.model.CSVInput;
import software.amazon.awssdk.services.s3.model.CSVOutput;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.ExpressionType;
import software.amazon.awssdk.services.s3.model.FileHeaderInfo;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.OutputSerialization;
import software.amazon.awssdk.services.s3.model.Progress;
import software.amazon.awssdk.services.s3.model.ScanRange;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;
import software.amazon.awssdk.services.s3.model.Stats;
import software.amazon.awssdk.utils.AttributeMap;

import io.oferto.pocminios3select.config.ObjectStorageConfig;
import io.oferto.pocminios3select.dto.ExpressionRequestDto;
import io.oferto.pocminios3select.model.Expression;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnnotationService {
	private final int CHUNK_SIZE = 10 * 1024 * 1024;
	
	private final ObjectStorageConfig objectStorageConfig;
		
	// disable cert validation
	private final AttributeMap attributeMap = AttributeMap.builder()
	        .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
	        //.put(SdkHttpConfigurationOption.GLOBAL_HTTP_DEFAULTS, Protocol.HTTPS)
	      .build();
	private final SdkHttpClient sdkHttpClient = new DefaultSdkHttpClientBuilder().buildWithDefaults(attributeMap);
	
	// async S3 Client
	private static final S3AsyncClient s3AsyncClient = S3AsyncClient.create();
	
	private SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, boolean isGzip, String query) {
        InputSerialization inputSerialization;        
        if (isGzip)
            inputSerialization = InputSerialization.builder()
        		.csv(CSVInput.builder()
        	        	.fileHeaderInfo(FileHeaderInfo.USE)
        	            .build())
        		.compressionType(CompressionType.GZIP)
        	.build();        	
        else 
            inputSerialization = InputSerialization.builder()
        		.csv(CSVInput.builder()
        	        	.fileHeaderInfo(FileHeaderInfo.USE)
        	            .build())
        		.compressionType(CompressionType.NONE)
        	.build();
        
        OutputSerialization outputSerialization = OutputSerialization.builder()
        		.csv(CSVOutput.builder()        	        	
        	            .build())
        		.build();      
        
        SelectObjectContentRequest request = SelectObjectContentRequest.builder()
	    	.bucket(bucket)
	    	.key(key)
	    	.expression(query)
	    	.expressionType(ExpressionType.SQL)
	    	.inputSerialization(inputSerialization)
	    	.outputSerialization(outputSerialization)
	    .build();
        
        return request;
    }
    private SelectObjectContentRequest generateBaseCSVRequestPaginated(String bucket, String key, boolean isGzip, String query, long startRange, long endRange) {
        InputSerialization inputSerialization;        
        if (isGzip)
            inputSerialization = InputSerialization.builder()
        		.csv(CSVInput.builder()
        	        	.fileHeaderInfo(FileHeaderInfo.USE)
        	            .build())
        		.compressionType(CompressionType.GZIP)
        	.build();        	
        else 
            inputSerialization = InputSerialization.builder()
        		.csv(CSVInput.builder()
        	        	.fileHeaderInfo(FileHeaderInfo.USE)
        	            .build())
        		.compressionType(CompressionType.NONE)
        	.build();
        
        OutputSerialization outputSerialization = OutputSerialization.builder()
        		.csv(CSVOutput.builder()        	        	
        	            .build())
        		.build();      
        
        SelectObjectContentRequest request= SelectObjectContentRequest.builder()
	    	.bucket(bucket)
	    	.key(key)
	    	.expression(query)
	    	.expressionType(ExpressionType.SQL)
	    	.inputSerialization(inputSerialization)
	    	.outputSerialization(outputSerialization)
	    	.scanRange(ScanRange.builder()
	    			.start(startRange)
	    			.end(endRange)
	    		.build())
	    .build();
        
        return request;
    }
    
    private static SelectObjectContentResponseHandler buildResponseHandler(EventStreamInfo eventStreamInfo) {
        // Use a Visitor to process the response stream. This visitor logs information and gathers details while processing.
        final SelectObjectContentResponseHandler.Visitor visitor = SelectObjectContentResponseHandler.Visitor.builder()
                .onRecords(r -> {
                    log.info("Record event received.");
                    eventStreamInfo.addRecord(r.payload().asUtf8String());
                    eventStreamInfo.incrementOnRecordsCalled();
                })
                .onCont(ce -> {
                	log.info("Continuation event received.");
                    eventStreamInfo.incrementContinuationEvents();
                })
                .onProgress(pe -> {
                    Progress progress = pe.details();
                    log.info("Progress event received:\n bytesScanned:{}\nbytesProcessed: {}\nbytesReturned:{}",
                            progress.bytesScanned(),
                            progress.bytesProcessed(),
                            progress.bytesReturned());
                })
                .onEnd(ee -> log.info("End event received."))
                .onStats(se -> {
                	log.info("Stats event received.");
                    eventStreamInfo.addStats(se.details());
                })
                .build();

        // Build the SelectObjectContentResponseHandler with the visitor that processes the stream.
        return SelectObjectContentResponseHandler.builder()
                .subscriber(visitor).build();
    }
    
    // The EventStreamInfo class is used to store information gathered while processing the response stream.
    static class EventStreamInfo {
        private final List<String> records = new ArrayList<>();
        private Integer countOnRecordsCalled = 0;
        private Integer countContinuationEvents = 0;
        private Stats stats;

        void incrementOnRecordsCalled() {
            countOnRecordsCalled++;
        }

        void incrementContinuationEvents() {
            countContinuationEvents++;
        }

        void addRecord(String record) {
            records.add(record);
        }

        void addStats(Stats stats) {
            this.stats = stats;
        }

        public List<String> getRecords() {
            return records;
        }

        public Integer getCountOnRecordsCalled() {
            return countOnRecordsCalled;
        }

        public Integer getCountContinuationEvents() {
            return countContinuationEvents;
        }

        public Stats getStats() {
            return stats;
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> List<T> convertToExpression(InputStream file, Class<T> responseType) {
        List<T> models;
        try (Reader reader = new BufferedReader(new InputStreamReader(file))) {           
			CsvToBean<?> csvToBean = new CsvToBeanBuilder(reader)
                    .withType(responseType)
                    .withIgnoreLeadingWhiteSpace(true)
                    .withIgnoreEmptyLine(true)
                    .build();
            models = (List<T>) csvToBean.parse();
        } catch (Exception ex) {
            log.error("error parsing csv file {} ", ex);
            throw new IllegalArgumentException(ex.getCause().getMessage());
        }
        
        return models;
    }
    
	public List<Expression> findAllExpressionsByAnnotation(ExpressionRequestDto expressionRequestDto) throws Exception {
		List<Expression> expressions = new ArrayList<Expression>();
		 
		log.debug("findAllExpressionsByAnnotation: found expressions from annotation Id: {}", expressionRequestDto.getAnnotationId());
		
		long start = System.currentTimeMillis();
		NumberFormat formatter = new DecimalFormat("#0.00000");
						
		StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
				AwsBasicCredentials.create(
						objectStorageConfig.getAccessKey(),
						objectStorageConfig.getSecretKey()));
		
		final S3Client s3Client = S3Client.builder()
				/*.endpointProvider(new S3EndpointProvider() {
                    @Override
                    public CompletableFuture<Endpoint> resolveEndpoint(S3EndpointParams endpointParams) {
                        return CompletableFuture.completedFuture(Endpoint.builder()
                                .url(URI.create("https://" + objectStorageConfig.getHost() + ":" + objectStorageConfig.getPort() + "/" + expressionRequestDto.getBucketName()))
                                .build());
                    }
                })*/
				.endpointOverride(URI.create("https://" + objectStorageConfig.getHost() + ":" + objectStorageConfig.getPort()))
				.region(Region.US_EAST_1)
				.forcePathStyle(true)			
				.httpClient(sdkHttpClient)
				.credentialsProvider(credentials)			
			.build();
				
		// request annotation expressions
		String query = "select s._1, s.\"" + expressionRequestDto.getAnnotationId() + "\" from s3object s";
		
		boolean isGzip = false;
		if (expressionRequestDto.getKeyObjectName().contains(".gz"))
			isGzip = true;
		
        final AtomicBoolean isResultComplete = new AtomicBoolean(false);

        /*HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(expressionRequestDto.getBucketName())
                .key(expressionRequestDto.getKeyObjectName())
                .build();
        
        HeadObjectResponse response = s3Client.headObject(headObjectRequest);
        long startTange = 0;
        long endRange = Math.min(CHUNK_SIZE, response.contentLength());*/
        	            
        SelectObjectContentRequest select = generateBaseCSVRequest(
        		expressionRequestDto.getBucketName(), 
        		expressionRequestDto.getKeyObjectName(), 
        		isGzip, 
        		query);
        
        EventStreamInfo eventStreamInfo = new EventStreamInfo();
        // Call the selectObjectContent method with the request and a response handler.
        // Supply an EventStreamInfo object to the response handler to gather records and information from the response.
                
        s3AsyncClient.selectObjectContent(select, buildResponseHandler(eventStreamInfo))
        	.join();
                    
        long recordCount = eventStreamInfo.getRecords().stream().mapToInt(record ->
        	record.split("\n").length
        ).sum();
              
		return expressions;			
	}
}

package io.oferto.pocminios3select.service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Service;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

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

import io.oferto.pocminios3select.config.ObjectStorageConfig;
import io.oferto.pocminios3select.config.S3AsyncClientConfig;
import io.oferto.pocminios3select.config.S3ClientConfig;
import io.oferto.pocminios3select.dto.ExpressionRequestDto;
import io.oferto.pocminios3select.model.Expression;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnnotationService {
	private final int CHUNK_SIZE = 10 * 1024 * 1024;
	  	
	private SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, boolean isGzip, String query) {
        InputSerialization inputSerialization;        
        if (isGzip)
            inputSerialization = InputSerialization.builder()
        		.csv(CSVInput.builder().fileHeaderInfo(FileHeaderInfo.USE).build())
        		.compressionType(CompressionType.GZIP)
        	.build();        	
        else 
            inputSerialization = InputSerialization.builder()
        		.csv(CSVInput.builder().fileHeaderInfo(FileHeaderInfo.USE).build())
        		.compressionType(CompressionType.NONE)
        	.build();
        
        OutputSerialization outputSerialization = OutputSerialization.builder()
        		.csv(CSVOutput.builder().build())
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
    
    private SelectObjectContentResponseHandler buildResponseHandler(EventStreamInfo eventStreamInfo) {
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
    private class EventStreamInfo {
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
    private <T> List<T> convertToExpression(InputStream file, Class<T> responseType) {
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
    
    private final S3ClientConfig s3ClientConfig;
    private final S3AsyncClientConfig s3AsyncClientConfig;
       
	public List<Expression> findAllExpressionsByAnnotation(ExpressionRequestDto expressionRequestDto) throws Exception {
		log.debug("findAllExpressionsByAnnotation: found expressions from annotation Id: {}", expressionRequestDto.getAnnotationId());
				 		
		NumberFormat formatter = new DecimalFormat("#0.00000");
		
		List<Expression> expressions = new ArrayList<Expression>();

		// request resource metadata
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
        		.bucket(expressionRequestDto.getBucketName())
                .key(expressionRequestDto.getKeyObjectName())
            .build();
        
        HeadObjectResponse response = s3ClientConfig.getS3Client().headObject(headObjectRequest);        
          
        // filter csv object
        long startTange = 0;
        long endRange = Math.min(CHUNK_SIZE, response.contentLength());
        
		String query = "select s._1, s.\"" + expressionRequestDto.getAnnotationId() + "\" from s3object s";
		
		boolean isGzip = false;
		if (expressionRequestDto.getKeyObjectName().contains(".gz"))
			isGzip = true;

        SelectObjectContentRequest select = generateBaseCSVRequest(
        		expressionRequestDto.getBucketName(), 
        		expressionRequestDto.getKeyObjectName(), 
        		isGzip, 
        		query);
                        
        // call the selectObjectContent method with the request and a response handler. Supply an EventStreamInfo object to the response handler to gather records and information from the response.
		long start = System.currentTimeMillis();
        
        EventStreamInfo eventStreamInfo = new EventStreamInfo();

        s3AsyncClientConfig.getS3AsyncClient()
        	.selectObjectContent(select, buildResponseHandler(eventStreamInfo)).join();
        
        // parsing async result to stream input and parse
        StringBuilder sb = new StringBuilder();
        for(String s : eventStreamInfo.getRecords()){
            sb.append(s);           
        }
        
        ByteArrayInputStream resultInputStream = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
        
        expressions = convertToExpression(resultInputStream, Expression.class);
        long end = System.currentTimeMillis();
        
        System.out.print("Execution Persist time is " + formatter.format((end - start) / 1000d) + " seconds");
        
		return expressions;			
	}
}

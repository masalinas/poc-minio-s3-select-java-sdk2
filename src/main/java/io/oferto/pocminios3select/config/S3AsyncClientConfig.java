package io.oferto.pocminios3select.config;

import java.net.URI;

import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.utils.AttributeMap;

import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class S3AsyncClientConfig {
	private final ObjectStorageConfig objectStorageConfig;
	
	private AttributeMap attributeMap = AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true).build();
	private SdkAsyncHttpClient sdkAsyncHttpClient = AwsCrtAsyncHttpClient.builder().buildWithDefaults(attributeMap);
	
	public S3AsyncClient getS3AsyncClient() {
		// configure object storage credentials
		StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
				AwsBasicCredentials.create(
						objectStorageConfig.getAccessKey(),
						objectStorageConfig.getSecretKey()));
		
		return S3AsyncClient.builder()   
        		.endpointOverride(URI.create("https://" + objectStorageConfig.getHost() + ":" + objectStorageConfig.getPort()))
        		.region(Region.US_EAST_1)
        		.forcePathStyle(true)    
        		.httpClient(sdkAsyncHttpClient)
        		.credentialsProvider(credentials)			
        	.build();
	}
}

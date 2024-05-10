package io.oferto.pocminios3select.config;

import java.net.URI;

import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.AttributeMap;

import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class S3ClientConfig {
	private final ObjectStorageConfig objectStorageConfig;
	
	// disable cert validation
	private final AttributeMap attributeMap = AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true).build();
	private final SdkHttpClient sdkHttpClient = new DefaultSdkHttpClientBuilder().buildWithDefaults(attributeMap);
	
	public S3Client getS3Client() {
		// configure object storage credentials
		StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
				AwsBasicCredentials.create(
						objectStorageConfig.getAccessKey(),
						objectStorageConfig.getSecretKey()));
		
		return S3Client.builder()
				.endpointOverride(URI.create("https://" + objectStorageConfig.getHost() + ":" + objectStorageConfig.getPort()))
				.region(Region.US_EAST_1)
				.forcePathStyle(true)			
				.httpClient(sdkHttpClient)
				.credentialsProvider(credentials)			
			.build();
	}
}

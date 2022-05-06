package br.com.grupo8.kafka.util;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class CredentialsUtil {
    public static final AwsCredentialsProvider credentialsProvider = new AwsCredentialsProvider() {
        @Override
        public AwsCredentials resolveCredentials() {
            return new AwsCredentials() {
                @Override
                public String accessKeyId() {
                    return System.getenv("AWS_ACCESS_KEY");
                }

                @Override
                public String secretAccessKey() {
                    return System.getenv("AWS_SECRET_KEY");
                }
            };
        }
    };
}

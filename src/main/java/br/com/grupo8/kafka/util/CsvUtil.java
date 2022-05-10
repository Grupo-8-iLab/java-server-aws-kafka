package br.com.grupo8.kafka.util;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.*;
import java.util.ArrayList;

public class CsvUtil {

    public static ArrayList<String[]> lerCsvProdutos (String arquivo) {
        BufferedReader reader = null;
        String line = "";
        ArrayList<String[]> dadosProdutos = new ArrayList<String[]>();

        try {
            reader = new BufferedReader(new FileReader(arquivo));
            while ((line = reader.readLine()) != null) {

                String[] row = line.split(";");
                dadosProdutos.add(row);
            }

            reader.close();
            return dadosProdutos;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void baixaArquivo(S3Client client, String bucket, String arquivo, String saida) throws IOException, NoSuchKeyException {

        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(arquivo)
                .build();

        ResponseInputStream<GetObjectResponse> responseResponseInputStream = client.getObject(request);

        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(saida));

        byte[] buffer = new  byte[4096];
        int bytesRead = -1;

        while ((bytesRead = responseResponseInputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
        }

        responseResponseInputStream.close();
        outputStream.close();
    }
}

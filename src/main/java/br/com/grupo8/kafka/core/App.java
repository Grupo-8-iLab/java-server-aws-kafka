package br.com.grupo8.kafka.core;

import br.com.grupo8.kafka.services.ICsvService;
import br.com.grupo8.kafka.util.CsvUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

@Component
@EnableScheduling
public class App {



    @Autowired
    private ICsvService serviceCsv;

    @Scheduled(fixedDelay = 1000*60*60*24)
    public void app() throws IOException, InterruptedException {
        String bucket = System.getenv("AWS_S3_BUCKET");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties("javaApplication"));
        consumer.subscribe(Collections.singletonList(System.getenv("KAFKA_TOPIC")));

        while (true){
            ConsumerRecords<String, String> registros = consumer.poll(Duration.ofMillis(250));
            //Thread.sleep(1000);
            for(ConsumerRecord<String, String> registro : registros) {
                String arquivo = registro.value();
                String endereco = "/tmp/"+arquivo;
                try {
                    CsvUtil.baixaArquivo(bucket, arquivo, endereco);
                    serviceCsv.salvaProdutosCsv(endereco);
                }catch (NoSuchKeyException ex) {
                    System.out.println("Nao foi possivel localizar o arquivo: " + arquivo);
                }
                 }
        }
    }

    private static Properties properties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_HOST"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId); // item que identifica qual consumidor irá ler a mensagem
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()); // para enviar dados em consumidores diferentes
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // para evitar conflito de partições e rebalanciamento
        return properties;
    }
}

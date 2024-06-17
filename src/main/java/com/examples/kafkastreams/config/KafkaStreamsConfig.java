package com.examples.kafkastreams.config;

import com.examples.kafkastreams.dto.FileStatus;
import com.examples.kafkastreams.dto.Payment;
import com.examples.kafkastreams.dto.PaymentValidationResult;
import com.examples.kafkastreams.dto.Test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
@EnableKafkaStreams
@EnableKafka
@Slf4j
public class KafkaStreamsConfig {

  @Autowired
  private KafkaProperties kafkaProperties;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  @Primary
  public KafkaStreamsConfiguration kStreamsConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-streams");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);
    props.put(JsonDeserializer.KEY_DEFAULT_TYPE, String.class);
    props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Test.class);
    return new KafkaStreamsConfiguration(props);
  }


  @Bean
  public KStream<String, String> kStream(StreamsBuilder builder) {
    KStream<String, String> fileStream = createFileParsingStream(builder);
    createPaymentValidationStream(builder); // will be part of other instance / application
    return fileStream;
  }

  private KStream<String, String> createFileParsingStream(StreamsBuilder builder) {
    KStream<String, String> fileStream = builder.stream("file-input-topic",
        Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, Payment> stringPaymentKStream = fileStream.flatMapValues(fileId -> {
      // Logic to read file and create payment objects
      List<Payment> payments = createPaymentsFromFile(fileId);
      return payments;
    });

    stringPaymentKStream.to("payment-validation-topic",
        Produced.with(Serdes.String(), new JsonSerde<>(Payment.class)));
    return fileStream;
  }

  private static Aggregator<String, PaymentValidationResult, FileStatus> getAggregator() {
    return (aggKey, newValue, aggValue) -> {
      log.info("current file status value {}", aggValue);
      aggValue.updateWith(newValue);
      aggValue.setFileId(newValue.getFileId());
      return aggValue;
    };
  }

  private void createPaymentValidationStream(StreamsBuilder builder) {
    KStream<String, Payment> paymentValidationStream = builder.stream("payment-validation-topic",
        Consumed.with(Serdes.String(), new JsonSerde<>(Payment.class)));

    KStream<String, PaymentValidationResult> validationResultStream = paymentValidationStream.mapValues(
        payment -> {
          boolean isValid = validate(payment);
          PaymentValidationResult result = new PaymentValidationResult();
          result.setFileId(payment.getFileId());
          result.setPaymentId(payment.getId());
          result.setValid(isValid);
          return result;
        });

    validationResultStream.foreach((k, v) -> log.info("Validated {} {} ", k, v));
    KTable<String, FileStatus> fileStatusKTable = validationResultStream.groupBy(
            (key, value) -> value.getFileId(),
            Grouped.with(Serdes.String(), new JsonSerde<>(PaymentValidationResult.class)))
        .aggregate(() -> new FileStatus(), getAggregator(),
            Materialized.<String, FileStatus, KeyValueStore<Bytes, byte[]>>as("filestatus")
                .withKeySerde(Serdes.String()).withValueSerde(new JsonSerde<>(FileStatus.class)));

    fileStatusKTable.toStream().foreach((k, v) -> log.info("v {} ", v));


  }

  public boolean validate(Payment payment) {
    log.info("validating payment {}", payment);
    return payment.getId() % 2 == 0;
  }

  private List<Payment> createPaymentsFromFile(String fileId) {
    List<Payment> paymentList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final Payment payment = new Payment();
      payment.setId(i);
      payment.setFileId(fileId);
      payment.setAmount(1 + 10);
      paymentList.add(payment);
    }
    return paymentList;
  }
}



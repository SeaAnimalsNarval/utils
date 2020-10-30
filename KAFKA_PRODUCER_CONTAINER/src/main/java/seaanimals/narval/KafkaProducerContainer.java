package seaanimals.narval;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import seaanimals.narval.domain.Pair;
import seaanimals.narval.exception.ProducerException;
import seaanimals.narval.producer.KafkaProducerDecorator;

/**
 * Apache Kafka decorators container
 * 
 * @author Narval
 * @param <K> key record type
 * @param <V> value record type
 */
public class KafkaProducerContainer<K, V> implements Closeable {

	private static Logger logger = LoggerFactory.getLogger(KafkaProducerContainer.class);

	private static final int WAIT_FUTURES_MS = 100;

	private final Map<String, Integer> kafkaProducerDecoratorUsed = new ConcurrentHashMap<String, Integer>();
	private final List<KafkaProducerDecorator<K, V>> listOfKafkaProducerDecorator;

	public KafkaProducerContainer(int totalProducers, Properties kafkaProducerProperties) {
		super();
		if (totalProducers < 1)
			throw new IllegalArgumentException("Total producers variable must be > 1");
		Objects.requireNonNull(kafkaProducerProperties);
		this.listOfKafkaProducerDecorator = (List<KafkaProducerDecorator<K, V>>) Collections
				.unmodifiableCollection(IntStream.range(0, totalProducers).boxed()
						.map(i -> new KafkaProducerDecorator<K, V>(kafkaProducerProperties))
						.collect(Collectors.toList()));
	}

	public KafkaProducerContainer(List<KafkaProducerDecorator<K, V>> listOfKafkaProducerDecorator) {
		Objects.requireNonNull(listOfKafkaProducerDecorator);
		if (listOfKafkaProducerDecorator.isEmpty())
			throw new IllegalArgumentException("List of kafka producers don't must to be empty");
		this.listOfKafkaProducerDecorator = listOfKafkaProducerDecorator;
	}

	/**
	 * Send record in kafka producer decorators
	 * 
	 * @param topic topic for produce record
	 * @param key   key record (may be null)
	 * @param value value record
	 * @throws ProducerException
	 * @return record metadata
	 */
	public RecordMetadata send(String topic, K key, V value) {
		Objects.requireNonNull(topic);
		Objects.requireNonNull(value);
		KafkaProducerDecorator<K, V> kafkaProducerDecorator;
		kafkaProducerDecorator = getKafkaProducerDecoratorWithMinOfMessagesInProcessing();
		kafkaProducerDecorator.addProcessingRecords(1);
		logger.debug("Kafka producer id: <{}>. Records in processing: <{}>", kafkaProducerDecorator.getId(),
				kafkaProducerDecorator.getProcessingRecords());
		try {
			return kafkaProducerDecorator.send(new ProducerRecord<K, V>(topic, key, value)).get();
		} catch (InterruptedException | ExecutionException ex) {
			throw new ProducerException(ex);
		} finally {
			kafkaProducerDecorator.decrementProcessingRecords(1);
			kafkaProducerDecoratorUsed.put(kafkaProducerDecorator.getId(),
					kafkaProducerDecorator.getTotalRecordsSent());
		}
	}

	/**
	 * Send records in kafka producer decorators
	 * 
	 * @param topic   topic for produce records
	 * @param records pairs of records
	 * @throws ProducerException
	 * @return records metadata
	 */
	public List<RecordMetadata> send(String topic, Collection<Pair<K, V>> records) {
		Objects.requireNonNull(topic);
		Objects.requireNonNull(records);
		KafkaProducerDecorator<K, V> kafkaProducerDecorator;
		kafkaProducerDecorator = getKafkaProducerDecoratorWithMinOfMessagesInProcessing();
		kafkaProducerDecorator.addProcessingRecords(records.size());
		logger.debug("Kafka producer id: <{}>. Records in processing: <{}>", kafkaProducerDecorator.getId(),
				kafkaProducerDecorator.getProcessingRecords());
		try {
			List<Future<RecordMetadata>> recordsFutureMetadata = kafkaProducerDecorator.send(topic, records);
			while (!isFutureDone(recordsFutureMetadata)) {
				try {
					Thread.sleep(WAIT_FUTURES_MS);
				} catch (InterruptedException e) {
				}
			}
			return recordsFutureMetadata.stream().map(f -> {
				try {
					return f.get();
				} catch (Exception ex) {
					throw new ProducerException(ex);
				}
			}).collect(Collectors.toList());
		} finally {
			kafkaProducerDecorator.decrementProcessingRecords(records.size());
			kafkaProducerDecoratorUsed.put(kafkaProducerDecorator.getId(),
					kafkaProducerDecorator.getTotalRecordsSent());
		}
	}

	public Map<String, Integer> getKafkaProducerDecoratorUsed() {
		return kafkaProducerDecoratorUsed;
	}

	private synchronized KafkaProducerDecorator<K, V> getKafkaProducerDecoratorWithMinOfMessagesInProcessing() {
		return listOfKafkaProducerDecorator.stream().sorted().findFirst().get();
	}

	private boolean isFutureDone(Collection<Future<RecordMetadata>> recordsFutureMetadata) {
		if (recordsFutureMetadata.isEmpty())
			return true;
		return recordsFutureMetadata.stream().allMatch(f -> f.isDone());
	}

	@Override
	public void close() throws IOException {
		for (KafkaProducerDecorator<K, V> kpd : listOfKafkaProducerDecorator)
			kpd.close();
	}

}

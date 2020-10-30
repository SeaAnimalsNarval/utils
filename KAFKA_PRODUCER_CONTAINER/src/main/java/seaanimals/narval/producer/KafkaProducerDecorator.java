package seaanimals.narval.producer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import seaanimals.narval.domain.Pair;

/**
 * Decorator over Apache Kafka producer.
 * 
 * @author Narval
 * @param <K> key record type
 * @param <V> value record type
 */
public class KafkaProducerDecorator<K, V> implements Producer<K, V>, Comparable<KafkaProducerDecorator<K, V>> {

	private final String id = UUID.randomUUID().toString();
	private final Producer<K, V> kafkaProducer;
	private volatile int recordsInProcessing = 0;
	private volatile int totalRecordsSent = 0;

	public KafkaProducerDecorator(Properties kafkaProducerProperties) {
		super();
		Objects.requireNonNull(kafkaProducerProperties);
		this.kafkaProducer = new KafkaProducer<K, V>(kafkaProducerProperties);
	}

	public KafkaProducerDecorator(Producer<K, V> kafkaProducer) {
		Objects.requireNonNull(kafkaProducer);
		this.kafkaProducer = kafkaProducer;
	}

	/**
	 * @param delta add to processing records
	 */
	public synchronized void addProcessingRecords(int delta) {
		recordsInProcessing += delta;
		if (totalRecordsSent + recordsInProcessing > Integer.MAX_VALUE)
			totalRecordsSent = 0;
		totalRecordsSent += recordsInProcessing;
	}

	/**
	 * @param delta decrement from processing records
	 */
	public synchronized void decrementProcessingRecords(int delta) {
		recordsInProcessing -= delta;
	}

	public int getProcessingRecords() {
		return recordsInProcessing;
	}

	public int getTotalRecordsSent() {
		return totalRecordsSent;
	}

	public String getId() {
		return id;
	}

	@Override
	public void initTransactions() {
		kafkaProducer.initTransactions();
	}

	@Override
	public void beginTransaction() throws ProducerFencedException {
		kafkaProducer.abortTransaction();
	}

	@Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
			throws ProducerFencedException {
		kafkaProducer.sendOffsetsToTransaction(offsets, consumerGroupId);
	}

	@Override
	public void commitTransaction() throws ProducerFencedException {
		kafkaProducer.commitTransaction();
	}

	@Override
	public void abortTransaction() throws ProducerFencedException {
		kafkaProducer.abortTransaction();
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
		Objects.requireNonNull(record);
		Objects.requireNonNull(record.topic());
		Objects.requireNonNull(record.value());
		return kafkaProducer.send(record);
	}

	/**
	 * @param topic   topic for produce records
	 * @param records pairs of records
	 * @return records metadata
	 */
	public List<Future<RecordMetadata>> send(String topic, Collection<Pair<K, V>> records) {
		Objects.requireNonNull(topic);
		Objects.requireNonNull(records);
		final List<Future<RecordMetadata>> result = new ArrayList<Future<RecordMetadata>>();
		records.stream().forEach(p -> result.add(kafkaProducer
				.send(new ProducerRecord<K, V>(topic, p.getLeft(), Objects.requireNonNull(p.getRight())))));
		return result;
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
		return kafkaProducer.send(record, callback);
	}

	@Override
	public void flush() {
		kafkaProducer.flush();
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return kafkaProducer.partitionsFor(topic);
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return kafkaProducer.metrics();
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		kafkaProducer.close(timeout, unit);
	}

	@Override
	public int compareTo(KafkaProducerDecorator<K, V> other) {
		return Integer.compare(recordsInProcessing, other.getProcessingRecords());
	}

	@Override
	public void close() {
		kafkaProducer.close();
	}

}

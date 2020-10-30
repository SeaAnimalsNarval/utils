package seaanimals.narval;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import seaanimals.narval.domain.Pair;
import seaanimals.narval.producer.KafkaProducerDecorator;

/**
 * @author Narval
 *
 */
public class KafkaProducerContainerTest {

	private static Logger logger = LoggerFactory.getLogger(KafkaProducerContainerTest.class);

	private static final String TOPIC = "TEST_TOPIC";
	private static final int TOTAL_THREADS_IN_POOL = Runtime.getRuntime().availableProcessors();
	private static final int TOTAL_KAFKA_PRODUCERS = Runtime.getRuntime().availableProcessors();
	private static final int SLEEPING_MILLIS_UPPER_BOUND = 100;
	private static final int TOTAL_TASK = 10000;
	private static final int TOTAL_MESSAGES_IN_TASK = 100;

	private KafkaProducerContainer<String, String> kafkaProducerContainer;

	@Before
	public void before() {
		kafkaProducerContainer = new KafkaProducerContainer<String, String>(
				IntStream.range(0, TOTAL_KAFKA_PRODUCERS).boxed()
						.map(i -> new KafkaProducerDecoratorWithRandomSleeping(SLEEPING_MILLIS_UPPER_BOUND,
								new MockProducer<String, String>(true, new StringSerializer(), new StringSerializer())))
						.collect(Collectors.toList()));
	}

	@Test
	public void recordProduceTest() throws InterruptedException {
		ExecutorService executorService = Executors.newFixedThreadPool(TOTAL_THREADS_IN_POOL);
		IntStream.range(0, TOTAL_TASK).forEach(i -> {
			executorService.submit(() -> {
				kafkaProducerContainer.send(TOPIC, IntStream.range(0, TOTAL_MESSAGES_IN_TASK).boxed()
						.map(k -> new Pair<String, String>(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
						.collect(Collectors.toList()));
			});
		});
		executorService.shutdown();
		executorService.awaitTermination(30, TimeUnit.SECONDS);
		logger.debug("Used Kafka producer ids: <{}>",
				Arrays.toString(kafkaProducerContainer.getKafkaProducerDecoratorUsed().entrySet().toArray()));
	}

	private class KafkaProducerDecoratorWithRandomSleeping extends KafkaProducerDecorator<String, String> {

		private final Random random = new Random();
		private final int sleepingMillisUpperBound;

		public KafkaProducerDecoratorWithRandomSleeping(int sleepingMillisUpperBound,
				Producer<String, String> kafkaProducer) {
			super(kafkaProducer);
			this.sleepingMillisUpperBound = sleepingMillisUpperBound;
		}

		@Override
		public List<Future<RecordMetadata>> send(String topic, Collection<Pair<String, String>> records) {
			try {
				Thread.sleep(random.nextInt(sleepingMillisUpperBound));
			} catch (InterruptedException ex) {
				logger.error("", ex);
			}
			return super.send(topic, records);
		}

	}

}

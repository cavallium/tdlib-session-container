package it.tdlight.reactiveapi;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Stats extends Thread {

	private static final Logger LOG = LogManager.getLogger(Stats.class);
	public static final List<ReactiveApiPublisher> STATS = new CopyOnWriteArrayList<>();
	public static final long SLEEP_INTERVAL = Duration.ofSeconds(10).toMillis();

	static {
		var stats = new Stats();
		stats.setName("Stats");
		stats.setDaemon(true);
		stats.start();
	}

	public static void init() {

	}

	@Override
	public void run() {
		try {
			var prev = System.currentTimeMillis();
			var prevClients = 0;
			var prevReceivedUpdates = new LongArrayList();
			var prevBufferedUpdates = new LongArrayList();
			var prevProcessedUpdates = new LongArrayList();
			var prevClientBoundEvents = new LongArrayList();
			var prevSentClientBoundEvents = new LongArrayList();
			while (!Thread.interrupted()) {
				//noinspection BusyWait
				Thread.sleep(SLEEP_INTERVAL);
				var now = System.currentTimeMillis();
				var timeDiffSeconds = (now - prev) / 1000d;
				StringBuilder out = new StringBuilder();
				out.append("Statistics. Time delta: %03.2fs%n".formatted(timeDiffSeconds));
				var currentClients = STATS.size();
				var clientIds = new LongArrayList();
				var receivedUpdates = new LongArrayList();
				var bufferedUpdates = new LongArrayList();
				var processedUpdates = new LongArrayList();
				var clientBoundEvents = new LongArrayList();
				var sentClientBoundEvents = new LongArrayList();
				for (ReactiveApiPublisher stat : STATS) {
					clientIds.add(stat.userId);
					receivedUpdates.add(stat.receivedUpdates.longValue());
					bufferedUpdates.add(stat.bufferedUpdates.longValue());
					processedUpdates.add(stat.processedUpdates.longValue());
					clientBoundEvents.add(stat.clientBoundEvents.longValue());
					sentClientBoundEvents.add(stat.sentClientBoundEvents.longValue());
				}
				while (currentClients > prevClients) {
					prevClients++;
					prevReceivedUpdates.add(0);
					prevBufferedUpdates.add(0);
					prevProcessedUpdates.add(0);
					prevClientBoundEvents.add(0);
					prevSentClientBoundEvents.add(0);
				}
				double receivedUpdatesRateSum = 0;
				long ramBufferedSum = 0;
				long diskBufferedSum = 0;
				double bufferedUpdatesRateSum = 0;
				double processedUpdatesRateSum = 0;
				double clientBoundEventsRateSum = 0;
				double sentClientBoundEventsRateSum = 0;
				for (int i = 0; i <= currentClients; i++) {
					double receivedUpdatesRate;
					long ramBuffered;
					long diskBuffered;
					double bufferedUpdatesRate;
					double processedUpdatesRate;
					double clientBoundEventsRate;
					double sentClientBoundEventsRate;
					if (i != currentClients) {
						receivedUpdatesRate = (receivedUpdates.getLong(i) - prevReceivedUpdates.getLong(i)) / timeDiffSeconds;
						diskBuffered = bufferedUpdates.getLong(i) - processedUpdates.getLong(i);
						ramBuffered = receivedUpdates.getLong(i) - bufferedUpdates.getLong(i);
						bufferedUpdatesRate = (bufferedUpdates.getLong(i) - prevBufferedUpdates.getLong(i)) / timeDiffSeconds;
						processedUpdatesRate = (processedUpdates.getLong(i) - prevProcessedUpdates.getLong(i)) / timeDiffSeconds;
						clientBoundEventsRate = (clientBoundEvents.getLong(i) - prevClientBoundEvents.getLong(i)) / timeDiffSeconds;
						sentClientBoundEventsRate =
								(sentClientBoundEvents.getLong(i) - prevSentClientBoundEvents.getLong(i)) / timeDiffSeconds;
						receivedUpdatesRateSum += receivedUpdatesRate;
						diskBufferedSum += diskBuffered;
						ramBufferedSum += ramBuffered;
						bufferedUpdatesRateSum += bufferedUpdatesRate;
						processedUpdatesRateSum += processedUpdatesRate;
						clientBoundEventsRateSum += clientBoundEventsRate;
						sentClientBoundEventsRateSum += sentClientBoundEventsRate;
						if (LOG.isTraceEnabled()) {
							out.append(String.format("%d:\t", clientIds.getLong(i)));
						}
					} else {
						receivedUpdatesRate = receivedUpdatesRateSum;
						diskBuffered = diskBufferedSum;
						ramBuffered = ramBufferedSum;
						bufferedUpdatesRate = bufferedUpdatesRateSum;
						processedUpdatesRate = processedUpdatesRateSum;
						clientBoundEventsRate = clientBoundEventsRateSum;
						sentClientBoundEventsRate = sentClientBoundEventsRateSum;
						out.append("Total:\t");
					}
					if (i == currentClients || LOG.isTraceEnabled()) {
						out.append(String.format(
								"\tUpdates:\t[received %03.2fHz\tbuffered: %03.2fHz (RAM: %d HDD: %d)\tprocessed: %03.2fHz]\tClient bound events: %03.2fHz\tProcessed events: %03.2fHz\t%n",
								receivedUpdatesRate,
								bufferedUpdatesRate,
								ramBuffered,
								diskBuffered,
								processedUpdatesRate,
								clientBoundEventsRate,
								sentClientBoundEventsRate
						));
					}
				}
				out.append(String.format("%n"));

				for (int i = 0; i < currentClients; i++) {
					prevReceivedUpdates = receivedUpdates;
					prevBufferedUpdates = bufferedUpdates;
					prevProcessedUpdates = processedUpdates;
					prevClientBoundEvents = clientBoundEvents;
					prevSentClientBoundEvents = sentClientBoundEvents;
				}
				LOG.debug(out.toString());

				prev = now;
			}
		} catch (InterruptedException ex) {

		}
	}
}

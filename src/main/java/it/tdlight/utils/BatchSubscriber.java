package it.tdlight.utils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static reactor.core.Exceptions.addSuppressed;
import static reactor.core.publisher.Operators.cancelledSubscription;
import static reactor.core.publisher.Operators.onErrorDropped;
import static reactor.core.publisher.Operators.onOperatorError;
import static reactor.core.publisher.Operators.setOnce;
import static reactor.core.publisher.Operators.terminate;
import static reactor.core.scheduler.Schedulers.parallel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

public abstract class BatchSubscriber<T> implements CoreSubscriber<T> {

	private static final Logger log = LoggerFactory.getLogger(BatchSubscriber.class);

	private final Scheduler scheduler;
	private final int batchSize;
	private final Duration timeout;

	private final BlockingQueue<T> buffer = new LinkedBlockingQueue<>();
	private final AtomicInteger requests = new AtomicInteger(0);

	private final AtomicReference<Disposable> flushTimer = new AtomicReference<>();
	private final Runnable flushTask = () -> {
		log.trace("timeout [{}] -> flush", buffer.size());
		flush();
	};

	private volatile Subscription subscription;
	private static AtomicReferenceFieldUpdater<BatchSubscriber, Subscription> S = newUpdater(BatchSubscriber.class, Subscription.class, "subscription");

	public BatchSubscriber(int batchSize, Duration timeout) {
		this.batchSize = batchSize;
		this.timeout = timeout;
		this.scheduler = parallel();
	}

	@Override
	public void onSubscribe(Subscription s) {
		setOnce(S, this, s);
	}

	@Override
	public void onNext(T record) {
		try {
			buffer.add(record);
			if (requests.get() > 0) {
				if (buffer.size() >= batchSize) {
					log.trace("+ value [{}] -> flush", buffer.size());
					flush();
				}
				else {
					log.trace("+ value [{}] -> flush in {}ms", buffer.size(), timeout.toMillis());
					scheduleFlush();
				}
			}
			else {
				log.trace("+ value [{}] -> buffer", buffer.size());
			}
		}
		catch (Throwable t) {
			onError(onOperatorError(subscription, t, record, currentContext()));
		}
	}

	@Override
	public void onError(Throwable t) {
		if (S.getAndSet(this, cancelledSubscription()) != cancelledSubscription()) {
			try {
				suspendFlush();
			}
			catch (Throwable e) {
				t = addSuppressed(e, t);
			}
		}

		onErrorDropped(t, currentContext());
	}

	@Override
	public void onComplete() {
		if (S.getAndSet(this, cancelledSubscription()) != cancelledSubscription()) {
			try {
				suspendFlush();
			}
			catch (Throwable e) { }
		}
	}

	// Implement what to do with a batch (either full or partial due to timeout).
	// Could be publish to another subscriber.
	public abstract void flush(List<T> batch);

	private void flush() {
		suspendFlush();

		List<T> batch = new ArrayList<>(batchSize);
		buffer.drainTo(batch, batchSize);
		flush(batch);

		requests.decrementAndGet();
		log.trace("- request [{}]", requests.get());
	}

	private void scheduleFlush() {
		flushTimer.updateAndGet(current -> {
			if (current != null) current.dispose();
			return scheduler.schedule(flushTask, timeout.toMillis(), MILLISECONDS);
		});
	}

	private void suspendFlush() {
		flushTimer.updateAndGet(current -> {
			if (current != null) current.dispose();
			return null;
		});
	}

	public void request() {
		if (requests.get() == 0 && buffer.size() >= batchSize) {
			log.trace(". request [{}] -> flush", buffer.size());
			flush();
		}
		else {
			int required = requests.incrementAndGet() == 1
					? batchSize - buffer.size()
					: batchSize;
			log.trace("+ request [{}] -> request {} values", buffer.size(), required);
			subscription.request(required);

			if (!buffer.isEmpty()) scheduleFlush();
		}
	}

	public void cancel() {
		terminate(S, this);
	}
}
package it.tdlight.reactiveapi;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

public class AtomixUtils {

	public static <T> Mono<T> fromCf(Supplier<? extends CompletableFuture<? extends T>> completableFutureSupplier) {
		return Mono.create(sink -> {
			var cf = completableFutureSupplier.get();
			cf.whenComplete((result, ex) -> {
				if (ex != null) {
					if (ex instanceof CompletionException) {
						sink.error(ex.getCause());
					} else {
						sink.error(ex);
					}
				} else if (result != null) {
					sink.success(result);
				} else {
					sink.success();
				}
			});
			sink.onCancel(() -> cf.cancel(true));
		});
	}
}

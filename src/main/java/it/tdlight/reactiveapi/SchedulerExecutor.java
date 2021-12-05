package it.tdlight.reactiveapi;

import java.util.concurrent.Executor;
import org.jetbrains.annotations.NotNull;
import reactor.core.scheduler.Scheduler;

public class SchedulerExecutor implements Executor {

	private final Scheduler scheduler;

	public SchedulerExecutor(Scheduler scheduler) {
		this.scheduler = scheduler;
	}

	@Override
	public void execute(@NotNull Runnable command) {
		scheduler.schedule(command);
	}
}

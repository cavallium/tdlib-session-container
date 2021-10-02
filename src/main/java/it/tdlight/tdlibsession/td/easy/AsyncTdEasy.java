package it.tdlight.tdlibsession.td.easy;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.AuthorizationState;
import it.tdlight.jni.TdApi.AuthorizationStateClosed;
import it.tdlight.jni.TdApi.AuthorizationStateClosing;
import it.tdlight.jni.TdApi.AuthorizationStateReady;
import it.tdlight.jni.TdApi.AuthorizationStateWaitCode;
import it.tdlight.jni.TdApi.AuthorizationStateWaitEncryptionKey;
import it.tdlight.jni.TdApi.AuthorizationStateWaitOtherDeviceConfirmation;
import it.tdlight.jni.TdApi.AuthorizationStateWaitPassword;
import it.tdlight.jni.TdApi.AuthorizationStateWaitPhoneNumber;
import it.tdlight.jni.TdApi.AuthorizationStateWaitRegistration;
import it.tdlight.jni.TdApi.AuthorizationStateWaitTdlibParameters;
import it.tdlight.jni.TdApi.CheckAuthenticationBotToken;
import it.tdlight.jni.TdApi.CheckAuthenticationCode;
import it.tdlight.jni.TdApi.CheckAuthenticationPassword;
import it.tdlight.jni.TdApi.CheckDatabaseEncryptionKey;
import it.tdlight.jni.TdApi.Error;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.jni.TdApi.Object;
import it.tdlight.jni.TdApi.OptionValueBoolean;
import it.tdlight.jni.TdApi.OptionValueEmpty;
import it.tdlight.jni.TdApi.OptionValueInteger;
import it.tdlight.jni.TdApi.OptionValueString;
import it.tdlight.jni.TdApi.PhoneNumberAuthenticationSettings;
import it.tdlight.jni.TdApi.RegisterUser;
import it.tdlight.jni.TdApi.SetAuthenticationPhoneNumber;
import it.tdlight.jni.TdApi.SetTdlibParameters;
import it.tdlight.jni.TdApi.TdlibParameters;
import it.tdlight.jni.TdApi.Update;
import it.tdlight.jni.TdApi.UpdateAuthorizationState;
import it.tdlight.tdlibsession.FatalErrorType;
import it.tdlight.tdlibsession.td.TdError;
import it.tdlight.tdlibsession.td.TdResult;
import it.tdlight.tdlibsession.td.middle.AsyncTdMiddle;
import it.tdlight.utils.MonoUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Publisher;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import org.warp.commonutils.error.InitializationException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@SuppressWarnings("unused")
public class AsyncTdEasy {

	private final Logger logger;
	private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(1);

	private final Many<AuthorizationState> authState = Sinks.many().replay().latest();
	private final AtomicBoolean requestedDefinitiveExit = new AtomicBoolean();
	private final AtomicReference<TdEasySettings> settings = new AtomicReference<>(null);
	private final Many<Error> globalErrors = Sinks.many().multicast().onBackpressureBuffer();
	private final One<FatalErrorType> fatalError = Sinks.one();
	private final AsyncTdMiddle td;
	private final String logName;
	private final Flux<Update> incomingUpdates;
	private final Scheduler scheduler = Schedulers.parallel();

	public AsyncTdEasy(AsyncTdMiddle td, String logName) {
		this.td = td;
		this.logName = logName;
		this.logger = LoggerFactory.getLogger("AsyncTdEasy " + logName);

		this.incomingUpdates = td.receive()
				.flatMapSequential(this::preprocessUpdates)
				.flatMapSequential(update -> Mono.from(this.getState()).single().map(state -> new AsyncTdUpdateObj(state, update)))
				.map(upd -> (TdApi.Update) upd.getUpdate())
				.doOnError(ex -> {
					if (ex instanceof TdError) {
						var tdEx = (TdError) ex;
						logger.error("Received an error update from telegram: " + tdEx.getTdCode() + " " + tdEx.getTdMessage());
						FatalErrorType fatalErrorType;
						try {
							fatalErrorType = FatalErrorType.valueOf(tdEx.getTdMessage());
						} catch (IllegalArgumentException ignored) {
							fatalErrorType = FatalErrorType.INVALID_UPDATE;
						}
						this.fatalError.tryEmitValue(fatalErrorType);
					} else {
						logger.error(ex.getLocalizedMessage(), ex);
					}
				})
				.doOnComplete(() -> authState.asFlux().take(1, true).single().subscribeOn(scheduler).subscribe(authState -> {
					onUpdatesTerminated();
					if (authState.getConstructor() != AuthorizationStateClosed.CONSTRUCTOR) {
						logger.warn("Updates stream has closed while"
								+ " the current authorization state is"
								+ " still {}. Setting authorization state as closed!", authState.getClass().getSimpleName());
						this.fatalError.tryEmitValue(FatalErrorType.CONNECTION_KILLED);
						this.authState.tryEmitNext(new AuthorizationStateClosed());
					}
				})).doOnError(ex -> authState.asFlux()
						.take(1, true)
						.single()
						.subscribeOn(scheduler)
						.subscribe(authState -> {
							onUpdatesTerminated();
							if (authState.getConstructor() != AuthorizationStateClosed.CONSTRUCTOR) {
								logger.warn("Updates stream has terminated with an error while"
										+ " the current authorization state is"
										+ " still {}. Setting authorization state as closed!", authState.getClass().getSimpleName());
								this.fatalError.tryEmitValue(FatalErrorType.CONNECTION_KILLED);
								this.authState.tryEmitNext(new AuthorizationStateClosed());
							}
						})
				);
	}

	private void onUpdatesTerminated() {
		logger.debug("Incoming updates flux terminated. Setting requestedDefinitiveExit: true");
		requestedDefinitiveExit.set(true);
	}

	public Mono<Void> create(TdEasySettings settings) {
		return Mono
				.fromCallable(() -> {
					// Create session directories
					if (Files.notExists(Path.of(settings.databaseDirectory))) {
						try {
							Files.createDirectories(Path.of(settings.databaseDirectory));
						} catch (IOException ex) {
							throw new InitializationException(ex);
						}
					}

					// Register fatal error handler
					fatalError.asMono().flatMap(settings.getFatalErrorHandler()::onFatalError).subscribeOn(scheduler).subscribe();

					return true;
				})
				.subscribeOn(Schedulers.boundedElastic())
				.flatMap(_v -> {
					this.settings.set(settings);
					return Mono.empty();
				})
				.then(td.initialize());
	}

	/**
	 * Get TDLib state
	 */
	public Flux<AuthorizationState> getState() {
		return authState.asFlux().distinct();
	}

	/**
	 * Get incoming updates from TDLib.
	 */
	public Flux<TdApi.Update> getIncomingUpdates() {
		return incomingUpdates;
	}

	/**
	 * Get generic error updates from TDLib (When they are not linked to a precise request).
	 */
	public Flux<TdApi.Error> getIncomingErrors() {
		return Flux.from(globalErrors.asFlux());
	}

	/**
	 * Receives fatal errors from TDLib.
	 */
	public Mono<FatalErrorType> getFatalErrors() {
		return Mono.from(fatalError.asMono());
	}

	/**
	 * Sends request to TDLib.
	 * @param timeout Timeout duration.
	 * @return The response or {@link TdApi.Error}.
	 */
	public <T extends Object> Mono<TdResult<T>> send(TdApi.Function request, Duration timeout) {
		return td.execute(request, timeout, false);
	}

	private <T extends TdApi.Object> Mono<TdResult<T>> sendDirectly(Function obj, boolean synchronous) {
		return td.execute(obj, AsyncTdEasy.DEFAULT_TIMEOUT, synchronous);
	}

	/**
	 * Set verbosity level
	 * @param i level
	 */
	public Mono<Void> setVerbosityLevel(int i) {
		return thenOrFatalError(sendDirectly(new TdApi.SetLogVerbosityLevel(i), true));
	}

	/**
	 * Clear option on TDLib
	 * @param name option name
	 */
	public Mono<Void> clearOption(String name) {
		return thenOrFatalError(sendDirectly(new TdApi.SetOption(name, new TdApi.OptionValueEmpty()), false
		));
	}

	/**
	 * Set option on TDLib
	 * @param name option name
	 * @param value option value
	 */
	public Mono<Void> setOptionString(String name, String value) {
		return thenOrFatalError(sendDirectly(new TdApi.SetOption(name, new TdApi.OptionValueString(value)), false
		));
	}

	/**
	 * Set option on TDLib
	 * @param name option name
	 * @param value option value
	 */
	public Mono<Void> setOptionInteger(String name, long value) {
		return thenOrFatalError(sendDirectly(new TdApi.SetOption(name, new TdApi.OptionValueInteger(value)), false
		));
	}

	/**
	 * Set option on TDLib
	 * @param name option name
	 * @param value option value
	 */
	public Mono<Void> setOptionBoolean(String name, boolean value) {
		return thenOrFatalError(sendDirectly(new TdApi.SetOption(name, new TdApi.OptionValueBoolean(value)), false
		));
	}

	/**
	 * Get option from TDLib
	 * @param name option name
	 * @return The value or nothing
	 */
	public Mono<String> getOptionString(String name) {
		return this
				.<TdApi.OptionValue>sendDirectly(new TdApi.GetOption(name), false)
				.<TdApi.OptionValue>handle(MonoUtils::orElseThrow)
				.flatMap(value -> {
			switch (value.getConstructor()) {
				case OptionValueString.CONSTRUCTOR:
					return Mono.just(((OptionValueString) value).value);
				case OptionValueEmpty.CONSTRUCTOR:
					return Mono.empty();
				default:
					return Mono.error(new UnsupportedOperationException("The option " + name + " is of type "
							+ value.getClass().getSimpleName()));
			}
		});
	}

	/**
	 * Get option from TDLib
	 * @param name option name
	 * @return The value or nothing
	 */
	public Mono<Long> getOptionInteger(String name) {
		return this
				.<TdApi.OptionValue>sendDirectly(new TdApi.GetOption(name), false)
				.<TdApi.OptionValue>handle(MonoUtils::orElseThrow)
				.flatMap(value -> {
					switch (value.getConstructor()) {
						case OptionValueInteger.CONSTRUCTOR:
							return Mono.just(((OptionValueInteger) value).value);
						case OptionValueEmpty.CONSTRUCTOR:
							return Mono.empty();
						default:
							return Mono.error(new UnsupportedOperationException(
									"The option " + name + " is of type " + value.getClass().getSimpleName()));
					}
				});
	}

	/**
	 * Get option from TDLib
	 * @param name option name
	 * @return The value or nothing
	 */
	public Mono<Boolean> getOptionBoolean(String name) {
		return this
				.<TdApi.OptionValue>sendDirectly(new TdApi.GetOption(name), false)
				.<TdApi.OptionValue>handle(MonoUtils::orElseThrow)
				.flatMap(value -> {
					switch (value.getConstructor()) {
						case OptionValueBoolean.CONSTRUCTOR:
							return Mono.just(((OptionValueBoolean) value).value);
						case OptionValueEmpty.CONSTRUCTOR:
							return Mono.empty();
						default:
							return Mono.error(new UnsupportedOperationException(
									"The option " + name + " is of type " + value.getClass().getSimpleName()));
					}
				});
	}

	/**
	 * Synchronously executes TDLib requests. Only a few requests can be executed synchronously. May
	 * be called from any thread.
	 *
	 * @param request Request to the TDLib.
	 * @param timeout Timeout.
	 * @return The request response.
	 */
	public <T extends Object> Mono<TdResult<T>> execute(TdApi.Function request, Duration timeout) {
		return td.execute(request, timeout, true);
	}

	/**
	 * Closes the client gracefully by sending {@link TdApi.Close}.
	 */
	public Mono<Void> close() {
		return Mono.from(getState())
				.filter(state -> {
					switch (state.getConstructor()) {
						case AuthorizationStateClosing.CONSTRUCTOR:
						case AuthorizationStateClosed.CONSTRUCTOR:
							return false;
						default:
							return true;
					}
				})
				.then(Mono.fromCallable(requestedDefinitiveExit::get).single())
				.filter(closeRequested -> !closeRequested)
				.doOnSuccess(s -> {
					logger.debug("Setting requestedDefinitiveExit: true");
					requestedDefinitiveExit.set(true);
				})
				.doOnSuccess(s -> logger.debug("Sending TdApi.Close"))
				.then(td.execute(new TdApi.Close(), DEFAULT_TIMEOUT, false))
				.doOnNext(closeResponse -> logger.debug("TdApi.Close response is: \"{}\"",
						closeResponse.toString().replace('\n', ' ')
				))
				.then(authState.asFlux()
						.filter(authorizationState -> authorizationState.getConstructor() == AuthorizationStateClosed.CONSTRUCTOR)
						.take(1)
						.singleOrEmpty())
				.doOnNext(ok -> logger.debug("Received AuthorizationStateClosed after TdApi.Close"))
				.doOnSuccess(s -> logger.info("AsyncTdEasy closed successfully"))
				.then();
	}

	private Mono<Update> catchErrors(Object obj) {
		return Mono.fromCallable(() -> {
			if (obj.getConstructor() == Error.CONSTRUCTOR) {
				var error = (Error) obj;

				switch (error.message) {
					case "PHONE_CODE_INVALID":
						globalErrors.tryEmitNext(error);
						return new UpdateAuthorizationState(new AuthorizationStateWaitCode());
					case "PASSWORD_HASH_INVALID":
						globalErrors.tryEmitNext(error);
						return new UpdateAuthorizationState(new AuthorizationStateWaitPassword());
					default:
						globalErrors.tryEmitNext(error);
						break;
				}
				analyzeFatalErrors(error);
				return null;
			} else {
				return (Update) obj;
			}
		});
	}

	private void analyzeFatalErrors(Object obj) {
		if (obj != null && obj.getConstructor() == Error.CONSTRUCTOR) {
			var error = (Error) obj;
			switch (error.message) {
				case "PHONE_NUMBER_INVALID":
					fatalError.tryEmitValue(FatalErrorType.PHONE_NUMBER_INVALID);
					break;
				case "ACCESS_TOKEN_INVALID":
					fatalError.tryEmitValue(FatalErrorType.ACCESS_TOKEN_INVALID);
					break;
				case "CONNECTION_KILLED":
					fatalError.tryEmitValue(FatalErrorType.CONNECTION_KILLED);
					break;
				case "INVALID_UPDATE":
					fatalError.tryEmitValue(FatalErrorType.INVALID_UPDATE);
					break;
				case "PHONE_NUMBER_BANNED":
					fatalError.tryEmitValue(FatalErrorType.PHONE_NUMBER_BANNED);
					break;
			}
		}
	}

	private Publisher<TdApi.Update> preprocessUpdates(TdApi.Object updateObj) {
		return Mono
				.just(updateObj)
				.flatMap(this::catchErrors)
				.filter(obj -> obj.getConstructor() == UpdateAuthorizationState.CONSTRUCTOR)
				.map(obj -> ((UpdateAuthorizationState) obj).authorizationState)
				.flatMap(obj -> {
					switch (obj.getConstructor()) {
						case AuthorizationStateWaitTdlibParameters.CONSTRUCTOR:
							return thenOrFatalError(Mono.fromCallable(this.settings::get).single().map(settings -> {
								var parameters = new TdlibParameters();
								parameters.useTestDc = settings.useTestDc;
								parameters.databaseDirectory = settings.databaseDirectory;
								parameters.filesDirectory = settings.filesDirectory;
								parameters.useFileDatabase = settings.useFileDatabase;
								parameters.useChatInfoDatabase = settings.useChatInfoDatabase;
								parameters.useMessageDatabase = settings.useMessageDatabase;
								parameters.useSecretChats = false;
								parameters.apiId = settings.apiId;
								parameters.apiHash = settings.apiHash;
								parameters.systemLanguageCode = settings.systemLanguageCode;
								parameters.deviceModel = settings.deviceModel;
								parameters.systemVersion = settings.systemVersion;
								parameters.applicationVersion = settings.applicationVersion;
								parameters.enableStorageOptimizer = settings.enableStorageOptimizer;
								parameters.ignoreFileNames = settings.ignoreFileNames;
								return new SetTdlibParameters(parameters);
							}).flatMap((SetTdlibParameters obj1) -> sendDirectly(obj1, false)));
						case AuthorizationStateWaitEncryptionKey.CONSTRUCTOR:
							return thenOrFatalError(sendDirectly(new CheckDatabaseEncryptionKey(), false))
									.onErrorResume((error) -> {
										logger.error("Error while checking TDLib encryption key", error);
										return sendDirectly(new TdApi.Close(), false).then();
									});
						case AuthorizationStateWaitPhoneNumber.CONSTRUCTOR:
							return thenOrFatalError(Mono.fromCallable(this.settings::get).single().flatMap(settings -> {
								if (settings.isPhoneNumberSet()) {
									return sendDirectly(new SetAuthenticationPhoneNumber(String.valueOf(settings.getPhoneNumber()),
											new PhoneNumberAuthenticationSettings(false, false, false)
									), false);
								} else if (settings.isBotTokenSet()) {
									return sendDirectly(new CheckAuthenticationBotToken(settings.getBotToken()), false);
								} else {
									return Mono.error(new IllegalArgumentException("A bot is neither an user or a bot"));
								}
							})).onErrorResume((error) -> {
								logger.error("Error while waiting for phone number", error);
								return sendDirectly(new TdApi.Close(), false).then();
							});
						case AuthorizationStateWaitRegistration.CONSTRUCTOR:
							var authorizationStateWaitRegistration = (AuthorizationStateWaitRegistration) obj;
							RegisterUser registerUser = new RegisterUser();
							if (authorizationStateWaitRegistration.termsOfService != null
									&& authorizationStateWaitRegistration.termsOfService.text != null && !authorizationStateWaitRegistration.termsOfService.text.text.isBlank()) {
								logger.info("Telegram Terms of Service:\n" + authorizationStateWaitRegistration.termsOfService.text.text);
							}

							return Mono
									.fromCallable(this.settings::get)
									.single()
									.map(TdEasySettings::getParameterRequestHandler)
									.flatMap(handler -> MonoUtils.thenOrLogRepeatError(() -> handler
											.onParameterRequest(Parameter.ASK_FIRST_NAME, new ParameterInfoEmpty())
											.filter(Objects::nonNull)
											.map(String::trim)
											.filter(firstName -> !firstName.isBlank() && firstName.length() <= 64 && firstName.length() > 0)
											.repeatWhen(s -> s.takeWhile(n -> n == 0))
											.last()
											.doOnNext(firstName -> registerUser.firstName = firstName)
											.then(handler
													.onParameterRequest(Parameter.ASK_LAST_NAME, new ParameterInfoEmpty())
													.filter(Objects::nonNull)
													.map(String::trim)
													.filter(lastName -> lastName.length() <= 64)
													.repeatWhen(s -> s.takeWhile(n -> n == 0))
													.last()
													.defaultIfEmpty("")
													.doOnNext(lastName -> registerUser.lastName = lastName)
											)
											.then(sendDirectly(registerUser, false))
									));
						case TdApi.AuthorizationStateWaitOtherDeviceConfirmation.CONSTRUCTOR:
							var authorizationStateWaitOtherDeviceConfirmation = (AuthorizationStateWaitOtherDeviceConfirmation) obj;
							return Mono
									.fromCallable(this.settings::get)
									.single()
									.map(TdEasySettings::getParameterRequestHandler)
									.flatMap(handler -> handler.onParameterRequest(Parameter.NOTIFY_LINK,
											new ParameterInfoNotifyLink(authorizationStateWaitOtherDeviceConfirmation.link)
									));
						case TdApi.AuthorizationStateWaitCode.CONSTRUCTOR:
							var authorizationStateWaitCode = (AuthorizationStateWaitCode) obj;
							return Mono
									.fromCallable(this.settings::get)
									.single()
									.map(TdEasySettings::getParameterRequestHandler)
									.flatMap(handler -> MonoUtils.thenOrLogRepeatError(() -> handler
											.onParameterRequest(Parameter.ASK_CODE, new ParameterInfoCode(authorizationStateWaitCode.codeInfo.phoneNumber,
													authorizationStateWaitCode.codeInfo.nextType,
													authorizationStateWaitCode.codeInfo.timeout,
													authorizationStateWaitCode.codeInfo.type))
											.flatMap(code -> sendDirectly(new CheckAuthenticationCode(code), false))
									));
						case AuthorizationStateWaitPassword.CONSTRUCTOR:
							var authorizationStateWaitPassword = (AuthorizationStateWaitPassword) obj;
							return Mono
									.fromCallable(this.settings::get)
									.single()
									.map(TdEasySettings::getParameterRequestHandler)
									.flatMap(handler -> MonoUtils.thenOrLogRepeatError(() -> handler
											.onParameterRequest(Parameter.ASK_PASSWORD, new ParameterInfoPasswordHint(
													authorizationStateWaitPassword.passwordHint))
											.flatMap(password -> sendDirectly(new CheckAuthenticationPassword(password), false))
									));
						case AuthorizationStateReady.CONSTRUCTOR: {
							this.authState.tryEmitNext(new AuthorizationStateReady());
							return Mono.empty();
						}
						case AuthorizationStateClosing.CONSTRUCTOR:
							logger.debug("Received AuthorizationStateClosing from td");
							return Mono.empty();
						case AuthorizationStateClosed.CONSTRUCTOR:
							logger.debug("Received AuthorizationStateClosed from td");
							return Mono.fromCallable(() -> {
								var closeRequested = this.requestedDefinitiveExit.get();
								if (closeRequested) {
									logger.debug("td closed successfully");
								} else {
									logger.warn("td closed unexpectedly: {}", logName);
								}
								authState.tryEmitNext(obj);
								return closeRequested;
							}).flatMap(closeRequested -> {
								if (closeRequested) {
									return Mono
											.fromCallable(settings::get)
											.single()
											.map(settings -> settings.databaseDirectory)
											.map(Path::of)
											.flatMapIterable(sessionPath -> Set.of(sessionPath.resolve("media"),
													sessionPath.resolve("passport"),
													sessionPath.resolve("profile_photos"),
													sessionPath.resolve("stickers"),
													sessionPath.resolve("temp"),
													sessionPath.resolve("thumbnails"),
													sessionPath.resolve("wallpapers")
											))
											.filterWhen(file -> Mono
													.fromCallable(() -> Files.exists(file))
													.subscribeOn(Schedulers.boundedElastic()))
											.doOnNext(directory -> {
												try {
													if (!Files.walk(directory)
															.sorted(Comparator.reverseOrder())
															.map(Path::toFile)
															.allMatch(File::delete)) {
														throw new IOException("Can't delete a file!");
													}
												} catch (IOException e) {
													logger.error("Can't delete temporary session subdirectory", e);
												}
											})
											.then(Mono.just(true));
								} else {
									return Mono.just(false);
								}
							}).then();
						default:
							return Mono.empty();
					}
				})
				.then(Mono.justOrEmpty(updateObj.getConstructor() == Error.CONSTRUCTOR ? null : (Update) updateObj));
	}

	public <T extends TdApi.Object> Mono<Void> thenOrFatalError(Mono<TdResult<T>> optionalMono) {
		return MonoUtils.thenOrError(optionalMono.doOnNext(result -> {
			if (result.failed()) {
				analyzeFatalErrors(result.cause());
			}
		}));
	}
}

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
import it.tdlight.jni.TdApi.Object;
import it.tdlight.jni.TdApi.OptionValue;
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
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.warp.commonutils.error.InitializationException;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class AsyncTdEasy {

	private static final Logger logger = LoggerFactory.getLogger(AsyncTdEasy.class);

	private final Scheduler scheduler = Schedulers.newSingle("AsyncTdEasy", false);
	private final ReplayProcessor<AuthorizationState> authState = ReplayProcessor.create(1);
	private final ReplayProcessor<Boolean> requestedDefinitiveExit = ReplayProcessor.cacheLastOrDefault(false);
	private final ReplayProcessor<TdEasySettings> settings = ReplayProcessor.cacheLast();
	private final EmitterProcessor<Error> globalErrors = EmitterProcessor.create();
	private final One<FatalErrorType> fatalError = Sinks.one();
	private final AsyncTdMiddle td;
	private final String logName;
	private final Flux<Update> incomingUpdates;

	public AsyncTdEasy(AsyncTdMiddle td, String logName) {
		this.td = td;
		this.logName = logName;

		// todo: use Duration.ZERO instead of 10ms interval
		this.incomingUpdates = td.receive()
				.flatMap(this::preprocessUpdates)
				.flatMap(update -> Mono.from(this.getState()).single().map(state -> new AsyncTdUpdateObj(state, update)))
				.filter(upd -> upd.getState().getConstructor() == AuthorizationStateReady.CONSTRUCTOR)
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
				}).doOnComplete(() -> {
					authState.asFlux().take(1).single().subscribe(authState -> {
						onUpdatesTerminated();
						if (authState.getConstructor() != AuthorizationStateClosed.CONSTRUCTOR) {
							logger.warn("Updates stream has closed while"
									+ " the current authorization state is"
									+ " still {}. Setting authorization state as closed!", authState.getClass().getSimpleName());
							this.authState.onNext(new AuthorizationStateClosed());
						}
					});
				}).doOnError(ex -> {
					authState.asFlux().take(1).single().subscribe(authState -> {
						onUpdatesTerminated();
						if (authState.getConstructor() != AuthorizationStateClosed.CONSTRUCTOR) {
							logger.warn("Updates stream has terminated with an error while"
									+ " the current authorization state is"
									+ " still {}. Setting authorization state as closed!", authState.getClass().getSimpleName());
							this.authState.onNext(new AuthorizationStateClosed());
						}
					});
				})
				.subscribeOn(scheduler)
				.publish().refCount(1);
	}

	private void onUpdatesTerminated() {
		logger.debug("Incoming updates flux terminated. Setting requestedDefinitiveExit: true");
		requestedDefinitiveExit.onNext(true);
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
				.publishOn(scheduler)
				.flatMap(_v -> {
					this.settings.onNext(settings);
					return Mono.empty();
				});
	}

	/**
	 * Get TDLib state
	 */
	public Flux<AuthorizationState> getState() {
		return Flux.from(authState).subscribeOn(scheduler);
	}

	/**
	 * Get incoming updates from TDLib.
	 */
	public Flux<TdApi.Update> getIncomingUpdates() {
		return getIncomingUpdates(false);
	}

	private Flux<TdApi.Update> getIncomingUpdates(boolean includePreAuthUpdates) {
		return incomingUpdates.subscribeOn(scheduler);
	}

	/**
	 * Get generic error updates from TDLib (When they are not linked to a precise request).
	 */
	public Flux<TdApi.Error> getIncomingErrors() {
		return Flux.from(globalErrors).subscribeOn(scheduler);
	}

	/**
	 * Receives fatal errors from TDLib.
	 */
	public Mono<FatalErrorType> getFatalErrors() {
		return Mono.from(fatalError.asMono()).subscribeOn(scheduler);
	}

	/**
	 * Sends request to TDLib.
	 * @return The response or {@link TdApi.Error}.
	 */
	public <T extends Object> Mono<TdResult<T>> send(TdApi.Function request) {
		return td.<T>execute(request, false);
	}

	private <T extends TdApi.Object> Mono<TdResult<T>> sendDirectly(TdApi.Function obj, boolean synchronous) {
		return td.<T>execute(obj, synchronous).subscribeOn(scheduler);
	}

	/**
	 * Set verbosity level
	 * @param i level
	 */
	public Mono<Void> setVerbosityLevel(int i) {
		return thenOrFatalError(sendDirectly(new TdApi.SetLogVerbosityLevel(i), true)).subscribeOn(scheduler);
	}

	/**
	 * Clear option on TDLib
	 * @param name option name
	 */
	public Mono<Void> clearOption(String name) {
		return thenOrFatalError(sendDirectly(new TdApi.SetOption(name, new TdApi.OptionValueEmpty()), false)).subscribeOn(scheduler);
	}

	/**
	 * Set option on TDLib
	 * @param name option name
	 * @param value option value
	 */
	public Mono<Void> setOptionString(String name, String value) {
		return thenOrFatalError(sendDirectly(new TdApi.SetOption(name, new TdApi.OptionValueString(value)), false)).subscribeOn(scheduler);
	}

	/**
	 * Set option on TDLib
	 * @param name option name
	 * @param value option value
	 */
	public Mono<Void> setOptionInteger(String name, long value) {
		return thenOrFatalError(sendDirectly(new TdApi.SetOption(name, new TdApi.OptionValueInteger(value)), false)).subscribeOn(scheduler);
	}

	/**
	 * Set option on TDLib
	 * @param name option name
	 * @param value option value
	 */
	public Mono<Void> setOptionBoolean(String name, boolean value) {
		return thenOrFatalError(sendDirectly(new TdApi.SetOption(name, new TdApi.OptionValueBoolean(value)), false)).subscribeOn(scheduler);
	}

	/**
	 * Get option from TDLib
	 * @param name option name
	 * @return The value or nothing
	 */
	public Mono<String> getOptionString(String name) {
		return this.<TdApi.OptionValue>sendDirectly(new TdApi.GetOption(name), false).<OptionValue>flatMap(MonoUtils::orElseThrow).flatMap((TdApi.OptionValue value) -> {
			switch (value.getConstructor()) {
				case OptionValueString.CONSTRUCTOR:
					return Mono.just(((OptionValueString) value).value);
				case OptionValueEmpty.CONSTRUCTOR:
					return Mono.empty();
				default:
					return Mono.error(new UnsupportedOperationException("The option " + name + " is of type "
							+ value.getClass().getSimpleName()));
			}
		}).subscribeOn(scheduler);
	}

	/**
	 * Get option from TDLib
	 * @param name option name
	 * @return The value or nothing
	 */
	public Mono<Long> getOptionInteger(String name) {
		return this.<TdApi.OptionValue>sendDirectly(new TdApi.GetOption(name), false).<TdApi.OptionValue>flatMap(MonoUtils::orElseThrow).flatMap((TdApi.OptionValue value) -> {
			switch (value.getConstructor()) {
				case OptionValueInteger.CONSTRUCTOR:
					return Mono.just(((OptionValueInteger) value).value);
				case OptionValueEmpty.CONSTRUCTOR:
					return Mono.empty();
				default:
					return Mono.error(new UnsupportedOperationException("The option " + name + " is of type "
							+ value.getClass().getSimpleName()));
			}
		}).subscribeOn(scheduler);
	}

	/**
	 * Get option from TDLib
	 * @param name option name
	 * @return The value or nothing
	 */
	public Mono<Boolean> getOptionBoolean(String name) {
		return this.<TdApi.OptionValue>sendDirectly(new TdApi.GetOption(name), false).<TdApi.OptionValue>flatMap(MonoUtils::orElseThrow).flatMap((TdApi.OptionValue value) -> {
			switch (value.getConstructor()) {
				case OptionValueBoolean.CONSTRUCTOR:
					return Mono.just(((OptionValueBoolean) value).value);
				case OptionValueEmpty.CONSTRUCTOR:
					return Mono.empty();
				default:
					return Mono.error(new UnsupportedOperationException("The option " + name + " is of type "
							+ value.getClass().getSimpleName()));
			}
		}).subscribeOn(scheduler);
	}

	/**
	 * Synchronously executes TDLib requests. Only a few requests can be executed synchronously. May
	 * be called from any thread.
	 *
	 * @param request Request to the TDLib.
	 * @return The request response.
	 */
	public <T extends Object> Mono<TdResult<T>> execute(TdApi.Function request) {
		return td.<T>execute(request, true).subscribeOn(scheduler);
	}

	/**
	 * Set if skip updates or not
	 */
	public Mono<Void> setSkipUpdates(boolean skipUpdates) { //todo: do this
		return null;
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
				.then(requestedDefinitiveExit.asFlux().take(1).single())
				.filter(closeRequested -> !closeRequested)
				.doOnSuccess(s -> {
					logger.debug("Setting requestedDefinitiveExit: true");
					requestedDefinitiveExit.onNext(true);
				})
				.then(td.execute(new TdApi.Close(), false).doOnSubscribe(s -> {
					logger.debug("Sending TdApi.Close");
				}))
				.doOnNext(closeResponse -> logger.debug("TdApi.Close response is: \"{}\"",
						closeResponse.toString().replace('\n', ' ')
				))
				.then(authState
						.filter(authorizationState -> authorizationState.getConstructor() == AuthorizationStateClosed.CONSTRUCTOR)
						.take(1)
						.singleOrEmpty())
				.doOnNext(ok -> {
					logger.debug("Received AuthorizationStateClosed after TdApi.Close");
				})
				.doOnSuccess(s -> {
					logger.info("AsyncTdEasy closed successfully");
				})
				.then()
				.subscribeOn(scheduler);
	}

	/**
	 *
	 * @param timeout Timeout in seconds when reading data
	 */
	public void setReadTimeout(int timeout) {
		//todo: do this
	}

	/**
	 *
	 * @param timeout Timeout in seconds when listening methods or connecting
	 */
	public void setMethodTimeout(int timeout) {
		//todo: do this
	}

	private Mono<Update> catchErrors(Object obj) {
		return Mono.<Update>fromCallable(() -> {
			if (obj.getConstructor() == Error.CONSTRUCTOR) {
				var error = (Error) obj;

				switch (error.message) {
					case "PHONE_CODE_INVALID":
						globalErrors.onNext(error);
						return new UpdateAuthorizationState(new AuthorizationStateWaitCode());
					case "PASSWORD_HASH_INVALID":
						globalErrors.onNext(error);
						return new UpdateAuthorizationState(new AuthorizationStateWaitPassword());
					default:
						globalErrors.onNext(error);
						break;
				}
				analyzeFatalErrors(error);
				return null;
			} else {
				return (Update) obj;
			}
		}).subscribeOn(scheduler);
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

	public Mono<Boolean> isBot() {
		return Mono.from(settings).single().map(TdEasySettings::isBotTokenSet).subscribeOn(scheduler);
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
							return thenOrFatalError(Mono.from(this.settings).map(settings -> {
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
							return thenOrFatalError(Mono.from(this.settings).flatMap(settings -> {
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
									.from(settings)
									.map(TdEasySettings::getParameterRequestHandler)
									.flatMap(handler -> {
										return MonoUtils.thenOrLogRepeatError(() -> handler
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
												.then(sendDirectly(registerUser, false)));
									});
						case TdApi.AuthorizationStateWaitOtherDeviceConfirmation.CONSTRUCTOR:
							var authorizationStateWaitOtherDeviceConfirmation = (AuthorizationStateWaitOtherDeviceConfirmation) obj;
							return Mono
									.from(settings)
									.map(TdEasySettings::getParameterRequestHandler)
									.flatMap(handler -> {
										return handler.onParameterRequest(Parameter.NOTIFY_LINK,
												new ParameterInfoNotifyLink(authorizationStateWaitOtherDeviceConfirmation.link)
										);
									});
						case TdApi.AuthorizationStateWaitCode.CONSTRUCTOR:
							var authorizationStateWaitCode = (AuthorizationStateWaitCode) obj;
							return Mono
									.from(settings)
									.map(TdEasySettings::getParameterRequestHandler)
									.flatMap(handler -> {
										return MonoUtils.thenOrLogRepeatError(() -> handler.onParameterRequest(Parameter.ASK_CODE,
												new ParameterInfoCode(authorizationStateWaitCode.codeInfo.phoneNumber,
														authorizationStateWaitCode.codeInfo.nextType,
														authorizationStateWaitCode.codeInfo.timeout,
														authorizationStateWaitCode.codeInfo.type
												)
										).flatMap(code -> sendDirectly(new CheckAuthenticationCode(code), false)));
									});
						case AuthorizationStateWaitPassword.CONSTRUCTOR:
							var authorizationStateWaitPassword = (AuthorizationStateWaitPassword) obj;
							return Mono
									.from(settings)
									.map(TdEasySettings::getParameterRequestHandler)
									.flatMap(handler -> {
										return MonoUtils.thenOrLogRepeatError(() -> handler.onParameterRequest(Parameter.ASK_PASSWORD,
												new ParameterInfoPasswordHint(authorizationStateWaitPassword.passwordHint)
										).flatMap(password -> sendDirectly(new CheckAuthenticationPassword(password), false)));
									});
						case AuthorizationStateReady.CONSTRUCTOR: {
							this.authState.onNext(new AuthorizationStateReady());
							return Mono.empty();
						}
						case AuthorizationStateClosing.CONSTRUCTOR:
							logger.debug("Received AuthorizationStateClosing from td");
							return Mono.empty();
						case AuthorizationStateClosed.CONSTRUCTOR:
							logger.debug("Received AuthorizationStateClosed from td");
							return Mono.from(requestedDefinitiveExit).doOnNext(closeRequested -> {
								if (closeRequested) {
									logger.debug("td closed successfully");
								} else {
									logger.warn("td closed unexpectedly: {}", logName);
								}
								authState.onNext(obj);
							}).flatMap(closeRequested -> {
								if (closeRequested) {
									return Mono
											.from(settings)
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
											.then(Mono.just(closeRequested));
								} else {
									return Mono.just(closeRequested);
								}
							}).then();
						default:
							return Mono.empty();
					}
				})
				.then(Mono.justOrEmpty(updateObj.getConstructor() == Error.CONSTRUCTOR ? null : (Update) updateObj))
				.subscribeOn(scheduler);
	}

	public <T extends TdApi.Object> Mono<Void> thenOrFatalError(Mono<TdResult<T>> optionalMono) {
		return MonoUtils.thenOrError(optionalMono.doOnNext(result -> {
			if (result.failed()) {
				analyzeFatalErrors(result.cause());
			}
		}));
	}
}

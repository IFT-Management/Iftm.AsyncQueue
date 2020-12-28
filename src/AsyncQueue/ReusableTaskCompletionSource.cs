using System;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Iftm.AsyncQueue {

    public abstract class ReusableTaskCompletionSourceBase : IThreadPoolWorkItem {
        private ExecutionContext? _executionContext;
        private Action<object?>? _scheduledContinuation;
        private object? _state;
        private SynchronizationContext? _synchronizationContext;
        private TaskScheduler? _taskScheduler;
        private protected Action<object?>? _continuation;

        private protected Exception? _exception;
        private protected short _token;

        private static void ResultSetMarkerFunc(object? _) { }
        private protected readonly static Action<object?> ResultSetMarker = ResultSetMarkerFunc;

        private static void ContinuationScheduledMarkerFunc(object? _) { }
        private protected readonly static Action<object?> ContinuationScheduledMarker = ContinuationScheduledMarkerFunc;

        private static void CallContinuation(object? thisObj) {
            var tcs = (ReusableTaskCompletionSourceBase)thisObj!;
            tcs._scheduledContinuation!(tcs._state);
        }

        private static readonly Action<object?> CallContinuationAction = CallContinuation;
        private static readonly SendOrPostCallback CallContinuationSendOrPost = CallContinuation;
        private static readonly ContextCallback CallContinuationContextCallback = CallContinuation;

        private static void CallContinuatonWithExecutionContext(object? thisObj) {
            var tcs = (ReusableTaskCompletionSourceBase)thisObj!;
            ExecutionContext.Run(tcs._executionContext!, CallContinuationContextCallback, thisObj);
        }

        private static readonly Action<object?> CallContinuatonWithExecutionContextAction = CallContinuatonWithExecutionContext;
        private static readonly SendOrPostCallback CallContinuatonWithExecutionContextSendOrPost = CallContinuatonWithExecutionContext;

        private static void ThrowResultAlreadySet() => throw new InvalidOperationException("Result already set.");
        private static void ThrowValueNotReadYet() => throw new InvalidOperationException("The previous value has not been read yet.");
        private static void ThrowInvalidOperation() => throw new InvalidOperationException();

        private protected static void ThrowPreviousTaskNotConsumed() => throw new InvalidOperationException("Attempting to get a new ValueTask before the previously obtained task was consumed.");
        private protected static void ThrowBadToken() => throw new ArgumentException("token");

        private protected void PublishResult(bool preferLocalContinuation) {
            Action<object?>? continuation = _continuation;
            bool continuationInitialized = false;

            for (; ; ) {
                if (continuation == null) {
                    var prevValue = Interlocked.CompareExchange(ref _continuation, ResultSetMarker, null);
                    if (prevValue == null) break;
                    else {
                        continuation = prevValue;
                        continuationInitialized = true;
                    }
                }
                else if (continuation == (object)ResultSetMarker) {
                    if (continuationInitialized) ThrowResultAlreadySet();
                    else {
                        continuation = Interlocked.CompareExchange(ref _continuation, null, null);
                        continuationInitialized = true;
                    }
                }
                else if (continuation == (object)ContinuationScheduledMarker) {
                    if (continuationInitialized) ThrowValueNotReadYet();
                    else {
                        continuation = Interlocked.CompareExchange(ref _continuation, null, null);
                        continuationInitialized = true;
                    }
                }
                else {
                    var prevValue = Interlocked.CompareExchange(ref _continuation, ContinuationScheduledMarker, continuation);
                    if (prevValue == continuation) {
                        _scheduledContinuation = continuation;

                        if (_synchronizationContext != null) {
                            var callback = _executionContext != null ? CallContinuatonWithExecutionContextSendOrPost : CallContinuationSendOrPost;
                            _synchronizationContext.Post(callback, this);
                        }
                        else if (_taskScheduler != null) {
                            var callback = _executionContext != null ? CallContinuatonWithExecutionContextAction : CallContinuationAction;
                            Task.Factory.StartNew(callback, this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, _taskScheduler);
                        }
                        else {
                            ThreadPool.UnsafeQueueUserWorkItem(this, preferLocalContinuation);
                        }

                        break;
                    }
                    else {
                        continuation = prevValue;
                        continuationInitialized = true;
                    }
                }
            }
        }

        public void SetCancelled(CancellationToken ct, bool preferLocalContinuation) {
            _exception = new OperationCanceledException(ct);
            PublishResult(preferLocalContinuation);
        }

        public void SetException(Exception ex, bool preferLocalContinuation) {
            _exception = ex;
            PublishResult(preferLocalContinuation);
        }

        private protected ValueTaskSourceStatus GetStatus(short token) {
            if (token != _token) ThrowBadToken();

            var continuation = Interlocked.CompareExchange(ref _continuation, null, null);

            return
                continuation != (object)ResultSetMarker && continuation != (object)ContinuationScheduledMarker ? ValueTaskSourceStatus.Pending :
                _exception is OperationCanceledException ? ValueTaskSourceStatus.Canceled :
                _exception != null ? ValueTaskSourceStatus.Faulted :
                ValueTaskSourceStatus.Succeeded;
        }

        private protected void OnCompleted(Action<object?> callback, object? state, short token, ValueTaskSourceOnCompletedFlags flags) {
            if (token != _token) ThrowBadToken();

            _synchronizationContext = null;
            _taskScheduler = null;

            if ((flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0) {
                _executionContext = ExecutionContext.Capture();
            }
            else {
                _executionContext = null;
            }

            if ((flags & ValueTaskSourceOnCompletedFlags.UseSchedulingContext) != 0) {
                var sc = SynchronizationContext.Current;
                if (sc != null && sc.GetType() != typeof(SynchronizationContext)) {
                    _synchronizationContext = sc;
                }
                else {
                    _synchronizationContext = null;

                    var ts = TaskScheduler.Current;
                    if (ts != null && ts != TaskScheduler.Default) _taskScheduler = ts;
                }
            }
            else {
                _synchronizationContext = null;
                _taskScheduler = null;
            }

            _state = state;

            Action<object?>? continuation = _continuation;
            bool continuationInitialized = false;

            for (; ; ) {
                if (continuation == null) {
                    var prevValue = Interlocked.CompareExchange(ref _continuation, callback, null);
                    if (prevValue == null) break;
                    else {
                        continuation = prevValue;
                        continuationInitialized = true;
                    }
                }
                else if (continuation == (object)ResultSetMarker) {
                    var prevValue = Interlocked.CompareExchange(ref _continuation, ContinuationScheduledMarker, ResultSetMarker);
                    if (prevValue == (object)ResultSetMarker) {
                        _scheduledContinuation = callback;

                        if (_synchronizationContext != null) {
                            _synchronizationContext.Post(CallContinuationSendOrPost, this);
                        }
                        else if (_taskScheduler != null) {
                            Task.Factory.StartNew(callback, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, _taskScheduler);
                        }
                        else {
                            ThreadPool.UnsafeQueueUserWorkItem(this, true);
                        }

                        break;
                    }
                    else {
                        continuation = prevValue;
                        continuationInitialized = true;
                    }
                }
                else {
                    if (continuationInitialized) ThrowInvalidOperation();
                    else {
                        continuation = Interlocked.CompareExchange(ref _continuation, null, null);
                        continuationInitialized = true;
                    }
                }
            }
        }

        void IThreadPoolWorkItem.Execute() {
            if (_executionContext != null) {
                ExecutionContext.Run(_executionContext, CallContinuationContextCallback, this);
            }
            else {
                _scheduledContinuation!(_state);
            }
        }
    }

    /// <summary>
    /// A reusable version of <see cref="TaskCompletionSource{TResult}"/>.
    /// 
    /// Represents an asynchronously read variable that can be read using <see cref="GetResultAsync"/> before it was written to
    /// by calling <see cref="SetResult(TResult)"/>, <see cref="ReusableTaskCompletionSourceBase.SetException(Exception)"/> or
    /// <see cref="ReusableTaskCompletionSourceBase.SetCancelled(CancellationToken)"/>.
    /// 
    /// In case it was written to before it's read, the <see cref="ValueTask{TResult}"/> returned from <see cref="GetResultAsync"/>
    /// will be immediately completed and available for reading.
    /// 
    /// Once the <see cref="ValueTask{TResult}"/> obtained from <see cref="GetResultAsync"/> is read, this instance is reinitialized
    /// and can be written to and read from again.
    /// 
    /// Attempting to write to this <see cref="ReusableTaskCompletionSource{TResult}"/> the second time before the first written variable was
    /// read is invalid and it will throw an <see cref="InvalidOperationException"/>. Attempting to read the value the second time by
    /// calling <see cref="GetResultAsync"/> before the previously returned <see cref="ValueTask{TResult}"/> has been read is invalid and
    /// it will throw an <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <typeparam name="TResult">Type of value to store.</typeparam>
    public sealed class ReusableTaskCompletionSource<TResult> : ReusableTaskCompletionSourceBase, IValueTaskSource<TResult> {
        private TResult _result = default(TResult)!;

        /// <summary>
        /// Sets the value to be read using a call to <see cref="GetResultAsync"/>.
        /// </summary>
        /// <param name="val">Value to be written.</param>
        public void SetResult(TResult val, bool preferLocalContinuation) {
            _result = val;
            PublishResult(preferLocalContinuation);
        }

        /// <summary>
        /// Returns a <see cref="ValueTask{TResult}"/> that contains either a previously written value,
        /// of if a value has not been written yet, a <see cref="ValueTask{TResult}"/> that will be signaled
        /// once a value has been written. This instance of <see cref="ReusableTaskCompletionSource{TResult}"/>
        /// will be reinitialized and available for subsequent reads and writes once the returned <see cref="ValueTask{TResult}"/>'s
        /// value is read.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown in case the result of <see cref="ValueTask{TResult}"/> from the
        /// previous call has not been read.</exception>
        /// <returns><see cref="ValueTask{TResult}"/></returns>
        public ValueTask<TResult> GetResultAsync() {
            Action<object?>? continuation = _continuation;
            bool continuationInitialized = false;

            for (; ; ) {
                if (continuation == (object)ResultSetMarker) {
                    var prevValue = Interlocked.CompareExchange(ref _continuation, ContinuationScheduledMarker, ResultSetMarker);
                    if (prevValue == (object)ResultSetMarker) {
                        var ret =
                            _exception is OperationCanceledException opc ? new ValueTask<TResult>(Task.FromCanceled<TResult>(opc.CancellationToken)) :
                            _exception != null ? new ValueTask<TResult>(Task.FromException<TResult>(_exception)) :
                            new ValueTask<TResult>(_result);

                        _exception = null;
                        _result = default!;

                        var expectContinuationScheduled = Interlocked.Exchange(ref _continuation, null);
                        Debug.Assert(expectContinuationScheduled == ContinuationScheduledMarker);

                        return ret;
                    }
                    else {
                        continuation = prevValue;
                        continuationInitialized = true;
                    }
                }
                else if (continuation == null) {
                    if (continuationInitialized) {
                        return new ValueTask<TResult>(this, ++_token);
                    }
                    else {
                        continuation = Interlocked.CompareExchange(ref _continuation, null, null);
                        continuationInitialized = true;
                    }
                }
                else {
                    if (continuationInitialized) ThrowPreviousTaskNotConsumed();
                    else {
                        continuation = Interlocked.CompareExchange(ref _continuation, null, null);
                        continuationInitialized = true;
                    }
                }
            }
        }

        TResult IValueTaskSource<TResult>.GetResult(short token) {
            if (token != _token) ThrowBadToken();

            if (_exception != null) {
                var ex = _exception;
                _exception = null;

                var prevValue = Interlocked.Exchange(ref _continuation, null);
                Debug.Assert(prevValue == ResultSetMarker || prevValue == ContinuationScheduledMarker);

                ExceptionDispatchInfo.Throw(ex);
                throw ex;
            }
            else {
                var ret = _result;
                _result = default!;

                var prevValue = Interlocked.Exchange(ref _continuation, null);
                Debug.Assert(prevValue == ResultSetMarker || prevValue == ContinuationScheduledMarker);

                return ret;
            }
        }

        ValueTaskSourceStatus IValueTaskSource<TResult>.GetStatus(short token) => GetStatus(token);

        void IValueTaskSource<TResult>.OnCompleted(Action<object?> callback, object? state, short token, ValueTaskSourceOnCompletedFlags flags) => OnCompleted(callback, state, token, flags);
    }

    /// <summary>
    /// A reusable version of <see cref="TaskCompletionSource{void}"/>.
    /// 
    /// Represents an asynchronously read variable that can be read using <see cref="GetResultAsync"/> before it was written to
    /// by calling <see cref="SetResult()"/>, <see cref="ReusableTaskCompletionSourceBase.SetException(Exception)"/> or
    /// <see cref="ReusableTaskCompletionSourceBase.SetCancelled(CancellationToken)"/>.
    /// 
    /// In case it was written to before it's read, the <see cref="ValueTask"/> returned from <see cref="GetResultAsync"/>
    /// will be immediately completed and available for reading.
    /// 
    /// Once the <see cref="ValueTask"/> obtained from <see cref="GetResultAsync"/> is read, this instance is reinitialized
    /// and can be written to and read from again.
    /// 
    /// Attempting to write to this <see cref="ReusableTaskCompletionSource"/> the second time before the first written variable was
    /// read is invalid and it will throw an <see cref="InvalidOperationException"/>. Attempting to read the value the second time by
    /// calling <see cref="GetResultAsync"/> before the previously returned <see cref="ValueTask"/> has been read is invalid and
    /// it will throw an <see cref="InvalidOperationException"/>.
    /// </summary>
    public sealed class ReusableTaskCompletionSource : ReusableTaskCompletionSourceBase, IValueTaskSource {
        /// <summary>
        /// Sets the value to be read using a call to <see cref="GetResultAsync"/>.
        /// </summary>
        public void SetResult(bool preferLocalContinuation) {
            PublishResult(preferLocalContinuation);
        }

        /// <summary>
        /// Returns a <see cref="ValueTask"/> that contains either a previously written value,
        /// of if a value has not been written yet, a <see cref="ValueTask"/> that will be signaled
        /// once a value has been written. This instance of <see cref="ReusableTaskCompletionSource"/>
        /// will be reinitialized and available for subsequent reads and writes once the returned <see cref="ValueTask"/>'s
        /// value is read.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown in case the result of <see cref="ValueTask"/> from the
        /// previous call has not been read.</exception>
        /// <returns><see cref="ValueTask"/></returns>
        public ValueTask GetResultAsync() {
            Action<object?>? continuation = _continuation;
            bool continuationInitialized = false;

            for (; ; ) {
                if (continuation == (object)ResultSetMarker) {
                    var prevValue = Interlocked.CompareExchange(ref _continuation, ContinuationScheduledMarker, ResultSetMarker);
                    if (prevValue == (object)ResultSetMarker) {
                        var ret =
                            _exception is OperationCanceledException opc ? new ValueTask(Task.FromCanceled(opc.CancellationToken)) :
                            _exception != null ? new ValueTask(Task.FromException(_exception)) :
                            new ValueTask();

                        _exception = null;

                        var expectContinuationScheduled = Interlocked.Exchange(ref _continuation, null);
                        Debug.Assert(expectContinuationScheduled == ContinuationScheduledMarker);

                        return ret;
                    }
                    else {
                        continuation = prevValue;
                    }
                }
                else if (continuation == null) {
                    if (continuationInitialized) return new ValueTask(this, ++_token);
                    else {
                        continuation = Interlocked.CompareExchange(ref _continuation, null, null);
                        continuationInitialized = true;
                    }
                }
                else {
                    if (continuationInitialized) ThrowPreviousTaskNotConsumed();
                    else {
                        continuation = Interlocked.CompareExchange(ref _continuation, null, null);
                        continuationInitialized = true;
                    }
                }
            }
        }

        void IValueTaskSource.GetResult(short token) {
            if (token != _token) ThrowBadToken();

            if (_exception != null) {
                var ex = _exception;
                _exception = null;

                var prevValue = Interlocked.Exchange(ref _continuation, null);
                Debug.Assert(prevValue == ResultSetMarker || prevValue == ContinuationScheduledMarker);

                ExceptionDispatchInfo.Throw(ex);
                throw ex;
            }
            else {
                var prevValue = Interlocked.Exchange(ref _continuation, null);
                Debug.Assert(prevValue == ResultSetMarker || prevValue == ContinuationScheduledMarker);
            }
        }

        ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => GetStatus(token);

        void IValueTaskSource.OnCompleted(Action<object?> callback, object? state, short token, ValueTaskSourceOnCompletedFlags flags) => OnCompleted(callback, state, token, flags);
    }
}

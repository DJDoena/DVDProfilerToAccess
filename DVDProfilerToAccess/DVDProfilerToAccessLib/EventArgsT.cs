namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System;

    public sealed class EventArgs<T> : EventArgs
    {
        public T Value { get; }

        public EventArgs(T value)
        {
            this.Value = value;
        }
    }
}
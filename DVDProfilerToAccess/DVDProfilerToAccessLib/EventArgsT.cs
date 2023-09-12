using System;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    public sealed class EventArgs<T> : EventArgs
    {
        public T Value { get; }

        public EventArgs(T value)
        {
            this.Value = value;
        }
    }
}
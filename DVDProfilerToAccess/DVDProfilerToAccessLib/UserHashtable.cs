namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using Profiler = DVDProfilerXML.Version400;

    internal sealed class UserHashtable : Hashtable<UserKey>
    {
        internal UserHashtable(int capacity) : base(capacity)
        { }

        internal void Add(Profiler.User user) => this.Add(new UserKey(user));

        internal bool ContainsKey(Profiler.User user) => this.ContainsKey(new UserKey(user));

        internal int this[Profiler.User user] => base[new UserKey(user)];
    }
}
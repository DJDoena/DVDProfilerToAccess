using Profiler = DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class UserDictionary : Dictionary<UserKey>
    {
        internal UserDictionary(int capacity) : base(capacity)
        { }

        internal void Add(Profiler.User user) => this.Add(new UserKey(user));

        internal bool ContainsKey(Profiler.User user) => this.ContainsKey(new UserKey(user));

        internal int this[Profiler.User user] => base[new UserKey(user)];
    }
}
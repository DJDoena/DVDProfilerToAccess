using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class UserHashtable : Hashtable<UserKey>
    {
        internal UserHashtable(int capacity)
            : base(capacity)
        { }

        internal void Add(User user)
        {
            Add(new UserKey(user));
        }

        internal bool ContainsKey(User user)
            => (ContainsKey(new UserKey(user)));

        internal int this[User user]
            => (base[new UserKey(user)]);
    }
}
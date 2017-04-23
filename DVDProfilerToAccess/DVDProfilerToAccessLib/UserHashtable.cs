using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class UserHashtable : Hashtable<UserKey>
    {
        internal UserHashtable(Int32 capacity)
            : base(capacity)
        { }

        internal void Add(User user)
        {
            Add(new UserKey(user));
        }

        internal Boolean ContainsKey(User user)
            => (ContainsKey(new UserKey(user)));

        internal Int32 this[User user]
            => (base[new UserKey(user)]);
    }
}
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using System;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class UserHashtable : Hashtable<UserKey>
    {
        internal UserHashtable(Int32 capacity)
            : base(capacity)
        {
        }

        internal void Add(User user)
        {
            base.Add(new UserKey(user));
        }

        internal Boolean ContainsKey(User user)
        {
            return (base.ContainsKey(new UserKey(user)));
        }

        internal Int32 this[User user]
        {
            get
            {
                return (base[new UserKey(user)]);
            }
        }
    }
}
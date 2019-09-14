using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class PersonHashtable : Hashtable<PersonKey>
    {
        internal PersonHashtable(int capacity)
            : base(capacity)
        { }

        internal void Add(IPerson person)
        {
            Add(new PersonKey(person));
        }

        internal bool ContainsKey(IPerson person)
            => (ContainsKey(new PersonKey(person)));

        internal int this[IPerson person]
            => (base[new PersonKey(person)]);
    }
}
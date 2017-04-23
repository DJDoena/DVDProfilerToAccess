using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class PersonHashtable : Hashtable<PersonKey>
    {
        internal PersonHashtable(Int32 capacity)
            : base(capacity)
        { }

        internal void Add(IPerson person)
        {
            Add(new PersonKey(person));
        }

        internal Boolean ContainsKey(IPerson person)
            => (ContainsKey(new PersonKey(person)));

        internal Int32 this[IPerson person]
            => (base[new PersonKey(person)]);
    }
}
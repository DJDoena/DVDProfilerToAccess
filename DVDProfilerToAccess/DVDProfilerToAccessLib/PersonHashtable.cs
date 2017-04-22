using DoenaSoft.DVDProfiler.DVDProfilerXML;
using System;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class PersonHashtable : Hashtable<PersonKey>
    {
        internal PersonHashtable(Int32 capacity)
            : base(capacity)
        {
        }

        internal void Add(IPerson person)
        {
            base.Add(new PersonKey(person));
        }

        internal Boolean ContainsKey(IPerson person)
        {
            return (base.ContainsKey(new PersonKey(person)));
        }

        internal Int32 this[IPerson person]
        {
            get
            {
                return (base[new PersonKey(person)]);
            }
        }
    }
}
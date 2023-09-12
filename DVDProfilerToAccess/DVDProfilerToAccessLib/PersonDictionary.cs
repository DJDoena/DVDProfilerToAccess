using DoenaSoft.DVDProfiler.DVDProfilerXML;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class PersonDictionary : Dictionary<PersonKey>
    {
        internal PersonDictionary(int capacity) : base(capacity)
        { }

        internal void Add(IPerson person) => this.Add(new PersonKey(person));

        internal bool ContainsKey(IPerson person) => this.ContainsKey(new PersonKey(person));

        internal int this[IPerson person] => base[new PersonKey(person)];
    }
}
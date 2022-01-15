namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using DVDProfilerXML;

    internal sealed class PersonHashtable : Hashtable<PersonKey>
    {
        internal PersonHashtable(int capacity) : base(capacity)
        { }

        internal void Add(IPerson person) => this.Add(new PersonKey(person));

        internal bool ContainsKey(IPerson person) => this.ContainsKey(new PersonKey(person));

        internal int this[IPerson person] => base[new PersonKey(person)];
    }
}
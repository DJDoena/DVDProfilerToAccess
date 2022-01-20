namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using JetEntityFrameworkProvider;

    public sealed class EntityProcessor : SqlProcessorBase
    {
        public EntityProcessor()
        {
            JetConnection.DUAL = JetConnection.DUALForMdb;
        }
    }
}

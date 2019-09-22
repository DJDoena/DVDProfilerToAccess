using System;
using System.ComponentModel;
using System.Diagnostics;
using Profiler = DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    [ImmutableObject(true)]
    [DebuggerDisplay("{FirstName} {LastName}")]
    internal sealed class UserKey : IEquatable<UserKey>
    {
        private readonly int _hashCode;

        public string LastName { get; }

        public string FirstName { get; }

        public string EmailAddress { get; }

        public string PhoneNumber { get; }

        public UserKey(Profiler.User user)
        {
            LastName = user.LastName ?? string.Empty;
            FirstName = user.FirstName ?? string.Empty;
            EmailAddress = user.EmailAddress;
            PhoneNumber = user.PhoneNumber;

            _hashCode = LastName.ToLowerInvariant().GetHashCode()
                ^ FirstName.ToLowerInvariant().GetHashCode();
        }

        public static bool IsValid(Profiler.User user) => !IsInvalid(user);

        public static bool IsInvalid(Profiler.User user)
        {
            if (user == null)
            {
                return true;
            }
            else if (string.IsNullOrEmpty(user.LastName) && string.IsNullOrEmpty(user.FirstName))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public override int GetHashCode() => _hashCode;

        public override bool Equals(object obj) => Equals(obj as UserKey);

        public bool Equals(UserKey other)
        {
            if (other == null)
            {
                return false;
            }

            var equals = string.Equals(LastName, other.LastName, StringComparison.InvariantCultureIgnoreCase)
                 && string.Equals(FirstName, other.FirstName, StringComparison.InvariantCultureIgnoreCase);

            return equals;
        }
    }
}
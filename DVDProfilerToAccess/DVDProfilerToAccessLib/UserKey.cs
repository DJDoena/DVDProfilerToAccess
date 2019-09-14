using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal class UserKey
    {
        private User m_User;

        private int m_HashCode;

        internal User User
        {
            get
            {
                User user = new User();

                user.EmailAddress = m_User.EmailAddress;
                user.FirstName = m_User.FirstName;
                user.LastName = m_User.LastName;
                user.PhoneNumber = m_User.PhoneNumber;

                return (user);
            }
        }

        internal UserKey(User user)
        {
            m_User = new User();

            m_User.EmailAddress = user.EmailAddress;
            m_User.FirstName = user.FirstName;
            m_User.LastName = user.LastName;
            m_User.PhoneNumber = user.PhoneNumber;

            m_HashCode = m_User.LastName.GetHashCode() / 2 + m_User.FirstName.GetHashCode() / 2;
        }

        public override int GetHashCode()
            => (m_HashCode);

        public override bool Equals(object obj)
        {
            UserKey other = obj as UserKey;

            if (other == null)
            {
                return (false);
            }
            else
            {
                return ((m_User.LastName == other.m_User.LastName) && (m_User.FirstName == other.m_User.FirstName));
            }
        }
    }
}
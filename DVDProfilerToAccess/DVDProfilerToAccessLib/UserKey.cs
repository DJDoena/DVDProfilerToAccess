using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using System;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal class UserKey
    {
        private User m_User;

        private Int32 m_HashCode;

        internal User User
        {
            get
            {
                User user;

                user = new User();
                user.EmailAddress = this.m_User.EmailAddress;
                user.FirstName = this.m_User.FirstName;
                user.LastName = this.m_User.LastName;
                user.PhoneNumber = this.m_User.PhoneNumber;
                return (user);
            }
        }

        internal UserKey(User user)
        {
            this.m_User = new User();
            this.m_User.EmailAddress = user.EmailAddress;
            this.m_User.FirstName = user.FirstName;
            this.m_User.LastName = user.LastName;
            this.m_User.PhoneNumber = user.PhoneNumber;
            this.m_HashCode = this.m_User.LastName.GetHashCode() / 2 + this.m_User.FirstName.GetHashCode() / 2;
        }

        public override Int32 GetHashCode()
        {
            return (this.m_HashCode);
        }

        public override Boolean Equals(Object obj)
        {
            UserKey other;

            other = obj as UserKey;
            if (other == null)
            {
                return (false);
            }
            else
            {
                return ((this.m_User.LastName == other.m_User.LastName) && (this.m_User.FirstName == other.m_User.FirstName));
            }
        }
    }
}
using DoenaSoft.DVDProfiler.DVDProfilerHelper;
using DoenaSoft.DVDProfiler.DVDProfilerXML;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using EN = DoenaSoft.DVDProfiler.EnhancedNotes;
using EPI = DoenaSoft.DVDProfiler.EnhancedPurchaseInfo;
using ET = DoenaSoft.DVDProfiler.EnhancedTitles;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    public sealed class SqlProcessor
    {
        #region Fields
        private static SqlProcessor s_Instance = new SqlProcessor();

        private const String DVDProfilerSchemaVersion = "3.9.2.0";
        private const String NULL = "NULL";
        internal Int32 IdCounter = 1;
        private Hashtable<String> AudioChannelsHash;
        private Hashtable<String> AudioContentHash;
        private Hashtable<String> AudioFormatHash;
        private Hashtable<String> CaseTypeHash;
        private CollectionTypeHashtable CollectionTypeHash;
        private Hashtable<EventType> EventTypeHash;
        private Hashtable<DVDID_Type> DVDIdTypeHash;
        private Hashtable<VideoStandard> VideoStandardHash;
        private Hashtable<String> GenreHash;
        private Hashtable<String> SubtitleHash;
        private Hashtable<String> MediaTypeHash;
        private PersonHashtable CastAndCrewHash;
        private Hashtable<String> StudioAndMediaCompanyHash;
        private TagHashtable TagHash;
        private UserHashtable UserHash;
        private Hashtable<CategoryRestriction> LinkCategoryHash;
        private Hashtable<String> CountryOfOriginHash;
        private Hashtable<String> LocalityHash;
        private PluginHashtable PluginHash;
        private Collection Collection;
        private OleDbCommand Command;
        #endregion

        private SqlProcessor()
        { }

        public static SqlProcessor Instance
        {
            get
            {
                return (s_Instance);
            }
        }

        public event EventHandler<EventArgs<Int32>> ProgressMaxChanged;

        public event EventHandler<EventArgs<Int32>> ProgressValueChanged;

        public event EventHandler<EventArgs<String>> Feedback;

        public ExceptionXml Process(String sourceFile
            , String targetFile)
        {
            OleDbConnection connection;
            OleDbTransaction transaction;
            ExceptionXml exceptionXml;

            connection = null;
            transaction = null;
            exceptionXml = null;
            try
            {
                DVDIdTypeHash = FillStaticHash<DVDID_Type>();
                EventTypeHash = FillStaticHash<EventType>();
                VideoStandardHash = FillStaticHash<VideoStandard>();
                LinkCategoryHash = FillStaticHash<CategoryRestriction>();

                Collection = Serializer<Collection>.Deserialize(sourceFile);

                FillDynamicHash();

                //Phase 3: Fill Basic Data Into Database
                if (File.Exists(targetFile))
                {
                    File.Delete(targetFile);
                }
                File.Copy("DVDProfiler.mdb", targetFile);
                File.SetAttributes(targetFile, FileAttributes.Normal | FileAttributes.Archive);
                using (connection = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + targetFile + ";Persist Security Info=True"))
                {
                    connection.Open();
                    using (transaction = connection.BeginTransaction())
                    {
                        using (Command = connection.CreateCommand())
                        {
                            Command.Transaction = transaction;

                            CheckDBVersion();

                            InsertBaseData(LocalityHash, "tLocality");
                            InsertBaseData(DVDIdTypeHash, "tDVDIdType");
                            InsertBaseData(AudioChannelsHash, "tAudioChannels");
                            InsertBaseData(AudioContentHash, "tAudioContent");
                            InsertBaseData(AudioFormatHash, "tAudioFormat");
                            InsertBaseData(CaseTypeHash, "tCaseType");
                            InsertBaseData(CollectionTypeHash, "tCollectionType");
                            InsertBaseData(EventTypeHash, "tEventType");
                            InsertBaseData(VideoStandardHash, "tVideoStandard");
                            InsertBaseData(GenreHash, "tGenre");
                            InsertBaseData(SubtitleHash, "tSubtitle");
                            InsertBaseData(MediaTypeHash, "tMediaType");
                            InsertBaseData(CastAndCrewHash, "tCastAndCrew");
                            InsertBaseData(LinkCategoryHash, "tLinkCategory");
                            InsertBaseData(CountryOfOriginHash, "tCountryOfOrigin");
                            InsertBaseData();

                            //Phase 4: Fill DVDs into Database
                            InsertData();

                            //Phase 5: Save & Exit
                            transaction.Commit();
                        }
                    }
                    connection.Close();
                }
                if (Collection.DVDList != null)
                {
                    if (Feedback != null)
                    {
                        Feedback(this, new EventArgs<String>(String.Format("{0:#,##0} profiles transformed.", Collection.DVDList.Length)));
                    }
                }
                if (Feedback != null)
                {
                    Feedback(this, new EventArgs<String>(String.Format("{0:#,##0} database entries created.", IdCounter)));
                }
            }
            catch (Exception exception)
            {
                try
                {
                    transaction.Rollback();
                }
                catch
                {
                }
                try
                {
                    connection.Close();
                }
                catch
                {
                }
                try
                {
                    if (File.Exists(targetFile))
                    {
                        File.Delete(targetFile);
                    }
                }
                catch
                {
                }
                if (Feedback != null)
                {
                    Feedback(this, new EventArgs<String>(String.Format("Error: {0} ", exception.Message)));
                }
                exceptionXml = new ExceptionXml(exception);
            }
            return (exceptionXml);
        }

        #region Fill...Hash
        private Hashtable<T> FillStaticHash<T>() where T : struct
        {
            FieldInfo[] fieldInfos;

            fieldInfos = typeof(T).GetFields(BindingFlags.Public | BindingFlags.Static);
            if (fieldInfos != null && fieldInfos.Length > 0)
            {
                Hashtable<T> hash;

                hash = new Hashtable<T>(fieldInfos.Length);
                foreach (FieldInfo fieldInfo in fieldInfos)
                {
                    hash.Add((T)(fieldInfo.GetRawConstantValue()));
                }
                return (hash);
            }
            else
            {
                return (new Hashtable<T>(0));
            }
        }

        private void FillDynamicHash()
        {
            InitializeHashes();

            if (Collection.DVDList != null && Collection.DVDList.Length > 0)
            {

                foreach (DVD dvd in Collection.DVDList)
                {
                    if (String.IsNullOrEmpty(dvd.ID))
                    {
                        continue;
                    }

                    FillLocalityHash(dvd);

                    FillCollectionTypeHash(dvd);

                    FillCastHash(dvd);

                    FillCrewHash(dvd);

                    FillUserHashFromLoanInfo(dvd);

                    FillUserHashFromEvents(dvd);

                    FillStudioHash(dvd);

                    FillMediaCompanyHash(dvd);

                    FillTagHash(dvd);

                    FillAudioHashes(dvd);

                    FillCaseTypeHash(dvd);

                    FillGenreHash(dvd);

                    FillSubtitleHash(dvd);

                    FillMediaTypeHash(dvd);

                    FillCountryOfOriginHash(dvd);

                    FillPluginHash(dvd);
                }
                foreach (DVD dvd in Collection.DVDList)
                {
                    //second iteration for data that is less complete
                    FillUserHashFromPurchaseInfo(dvd);
                }
            }
        }

        private void InitializeHashes()
        {
            LocalityHash = new Hashtable<String>(5);
            CollectionTypeHash = new CollectionTypeHashtable(5);
            CastAndCrewHash = new PersonHashtable(Collection.DVDList.Length * 50);
            StudioAndMediaCompanyHash = new Hashtable<String>(100);
            AudioChannelsHash = new Hashtable<String>(20);
            AudioContentHash = new Hashtable<String>(20);
            AudioFormatHash = new Hashtable<String>(20);
            CaseTypeHash = new Hashtable<String>(20);
            TagHash = new TagHashtable(50);
            UserHash = new UserHashtable(20);
            GenreHash = new Hashtable<String>(30);
            SubtitleHash = new Hashtable<String>(30);
            MediaTypeHash = new Hashtable<String>(5);
            CountryOfOriginHash = new Hashtable<String>(20);
            PluginHash = new PluginHashtable(5);
        }

        private void FillUserHashFromPurchaseInfo(DVD dvd)
        {
            if ((dvd.PurchaseInfo != null) && (dvd.PurchaseInfo.GiftFrom != null))
            {
                if ((String.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.FirstName) == false)
                    || (String.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.LastName) == false))
                {
                    User user;

                    user = new User(dvd.PurchaseInfo.GiftFrom);
                    if (UserHash.ContainsKey(user) == false)
                    {
                        UserHash.Add(user);
                    }
                }
            }
        }

        private void FillPluginHash(DVD dvd)
        {
            if ((dvd.PluginCustomData != null) && (dvd.PluginCustomData.Length > 0))
            {
                foreach (PluginData pluginData in dvd.PluginCustomData)
                {
                    if ((pluginData != null) && (PluginHash.ContainsKey(pluginData) == false))
                    {
                        PluginHash.Add(pluginData);
                    }
                }
            }
        }

        private void FillCountryOfOriginHash(DVD dvd)
        {
            if ((String.IsNullOrEmpty(dvd.CountryOfOrigin) == false) && (CountryOfOriginHash.ContainsKey(dvd.CountryOfOrigin) == false))
            {
                CountryOfOriginHash.Add(dvd.CountryOfOrigin);
            }
            if ((String.IsNullOrEmpty(dvd.CountryOfOrigin2) == false) && (CountryOfOriginHash.ContainsKey(dvd.CountryOfOrigin2) == false))
            {
                CountryOfOriginHash.Add(dvd.CountryOfOrigin2);
            }
            if ((String.IsNullOrEmpty(dvd.CountryOfOrigin3) == false) && (CountryOfOriginHash.ContainsKey(dvd.CountryOfOrigin3) == false))
            {
                CountryOfOriginHash.Add(dvd.CountryOfOrigin3);
            }
        }

        private void FillMediaTypeHash(DVD dvd)
        {
            if (dvd.MediaTypes != null)
            {
                if ((dvd.MediaTypes.DVD) && (MediaTypeHash.ContainsKey("DVD") == false))
                {
                    MediaTypeHash.Add("DVD");
                }
                if ((dvd.MediaTypes.BluRay) && (MediaTypeHash.ContainsKey("Blu-ray") == false))
                {
                    MediaTypeHash.Add("Blu-ray");
                }
                if ((dvd.MediaTypes.HDDVD) && (MediaTypeHash.ContainsKey("HD-DVD") == false))
                {
                    MediaTypeHash.Add("HD-DVD");
                }
                if ((String.IsNullOrEmpty(dvd.MediaTypes.CustomMediaType) == false)
                    && (MediaTypeHash.ContainsKey(dvd.MediaTypes.CustomMediaType) == false))
                {
                    MediaTypeHash.Add(dvd.MediaTypes.CustomMediaType);
                }
            }
        }

        private void FillSubtitleHash(DVD dvd)
        {
            if ((dvd.SubtitleList != null) && (dvd.SubtitleList.Length > 0))
            {
                foreach (String subtitle in dvd.SubtitleList)
                {
                    if ((String.IsNullOrEmpty(subtitle) == false) && (SubtitleHash.ContainsKey(subtitle) == false))
                    {
                        SubtitleHash.Add(subtitle);
                    }
                }
            }
        }

        private void FillGenreHash(DVD dvd)
        {
            if ((dvd.GenreList != null) && (dvd.GenreList.Length > 0))
            {
                foreach (String genre in dvd.GenreList)
                {
                    if ((String.IsNullOrEmpty(genre) == false) && (GenreHash.ContainsKey(genre) == false))
                    {
                        GenreHash.Add(genre);
                    }
                }
            }
        }

        private void FillCaseTypeHash(DVD dvd)
        {
            if (String.IsNullOrEmpty(dvd.CaseType) == false)
            {
                if (CaseTypeHash.ContainsKey(dvd.CaseType) == false)
                {
                    CaseTypeHash.Add(dvd.CaseType);
                }
            }
        }

        private void FillAudioHashes(DVD dvd)
        {
            if ((dvd.AudioList != null) && (dvd.AudioList.Length > 0))
            {
                foreach (AudioTrack audioTrack in dvd.AudioList)
                {
                    if (AudioContentHash.ContainsKey(audioTrack.Content) == false)
                    {
                        AudioContentHash.Add(audioTrack.Content);
                    }
                    if (AudioFormatHash.ContainsKey(audioTrack.Format) == false)
                    {
                        AudioFormatHash.Add(audioTrack.Format);
                    }
                    if (AudioChannelsHash.ContainsKey(audioTrack.Channels) == false)
                    {
                        AudioChannelsHash.Add(audioTrack.Channels);
                    }

                }
            }
        }

        private void FillTagHash(DVD dvd)
        {
            if (dvd.TagList != null && dvd.TagList.Length > 0)
            {
                foreach (Tag tag in dvd.TagList)
                {
                    if (TagHash.ContainsKey(tag) == false)
                    {
                        TagHash.Add(tag);
                    }
                }
            }
        }

        private void FillMediaCompanyHash(DVD dvd)
        {
            if (dvd.MediaCompanyList != null && dvd.MediaCompanyList.Length > 0)
            {
                foreach (String distributor in dvd.MediaCompanyList)
                {
                    if (StudioAndMediaCompanyHash.ContainsKey(distributor) == false)
                    {
                        StudioAndMediaCompanyHash.Add(distributor);
                    }
                }
            }
        }

        private void FillStudioHash(DVD dvd)
        {
            if (dvd.StudioList != null && dvd.StudioList.Length > 0)
            {
                foreach (String studio in dvd.StudioList)
                {
                    if (StudioAndMediaCompanyHash.ContainsKey(studio) == false)
                    {
                        StudioAndMediaCompanyHash.Add(studio);
                    }
                }
            }
        }

        private void FillUserHashFromEvents(DVD dvd)
        {
            if (dvd.EventList != null && dvd.EventList.Length > 0)
            {
                foreach (Event myEvent in dvd.EventList)
                {
                    if (UserHash.ContainsKey(myEvent.User) == false)
                    {
                        UserHash.Add(myEvent.User);
                    }
                }
            }
        }

        private void FillUserHashFromLoanInfo(DVD dvd)
        {
            if (dvd.LoanInfo != null && dvd.LoanInfo.User != null)
            {
                if (UserHash.ContainsKey(dvd.LoanInfo.User) == false)
                {
                    UserHash.Add(dvd.LoanInfo.User);
                }
            }
        }

        private void FillCrewHash(DVD dvd)
        {
            if (dvd.CrewList != null && dvd.CrewList.Length > 0)
            {
                foreach (Object possibleCrew in dvd.CrewList)
                {
                    FillDynamicHash<CrewMember>(CastAndCrewHash, possibleCrew);
                }
            }
        }

        private void FillCastHash(DVD dvd)
        {
            if (dvd.CastList != null && dvd.CastList.Length > 0)
            {
                foreach (Object possibleCast in dvd.CastList)
                {
                    FillDynamicHash<CastMember>(CastAndCrewHash, possibleCast);
                }
            }
        }

        private void FillCollectionTypeHash(DVD dvd)
        {
            if (CollectionTypeHash.ContainsKey(dvd.CollectionType) == false)
            {
                CollectionTypeHash.Add(dvd.CollectionType);
            }
        }

        private void FillLocalityHash(DVD dvd)
        {
            if (LocalityHash.ContainsKey(dvd.ID_LocalityDesc) == false)
            {
                LocalityHash.Add(dvd.ID_LocalityDesc);
            }
        }

        private void FillDynamicHash<T>(PersonHashtable personHash, Object possiblePerson) where T : class, IPerson
        {
            T person;

            person = possiblePerson as T;
            if (person != null)
            {
                if (personHash.ContainsKey(person) == false)
                {
                    personHash.Add(person);
                }
            }
        }
        #endregion

        #region GetInsert...Command(s)
        private void GetInsertPluginDataCommands(List<String> sqlCommands)
        {
            foreach (KeyValuePair<PluginKey, Int32> keyValue in PluginHash)
            {
                GetInsertPluginDataCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertTagCommands(List<String> sqlCommands)
        {
            foreach (KeyValuePair<TagKey, Int32> keyValue in TagHash)
            {
                GetInsertTagCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertStudioAndMediaCompanyCommands(List<String> sqlCommands)
        {
            foreach (KeyValuePair<String, Int32> keyValue in StudioAndMediaCompanyHash)
            {
                GetInsertStudioAndMediaCompanyCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertUserCommands(List<String> sqlCommands)
        {
            foreach (KeyValuePair<UserKey, Int32> keyValue in UserHash)
            {
                GetInsertUserCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertPluginDataCommand(List<String> sqlCommands, KeyValuePair<PluginKey, Int32> keyValue)
        {

            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tPluginData VALUES (");
            insertCommand.Append(keyValue.Value.ToString());
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(keyValue.Key.PluginData.ClassID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.PluginData.Name));
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertTagCommand(List<String> sqlCommands, KeyValuePair<TagKey, Int32> keyValue)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tTag VALUES (");
            insertCommand.Append(keyValue.Value.ToString());
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.Tag.Name));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.Tag.FullName));
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertStudioAndMediaCompanyCommand(List<String> sqlCommands, KeyValuePair<String, Int32> keyValue)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tStudioAndMediaCompany VALUES (");
            insertCommand.Append(keyValue.Value.ToString());
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key));
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertUserCommand(List<String> sqlCommands, KeyValuePair<UserKey, Int32> keyValue)
        {
            StringBuilder insertCommand;
            User user;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tUser VALUES (");
            insertCommand.Append(keyValue.Value.ToString());
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.User.LastName));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.User.FirstName));
            insertCommand.Append(", ");
            user = keyValue.Key.User as User;
            if (user != null)
            {
                insertCommand.Append(PrepareOptionalTextForDb(user.EmailAddress));
                insertCommand.Append(", ");
                insertCommand.Append(PrepareOptionalTextForDb(user.PhoneNumber));
            }
            else
            {
                insertCommand.Append(NULL);
                insertCommand.Append(", ");
                insertCommand.Append(NULL);
            }
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private List<String> GetInsertBaseDataCommands(Hashtable<String> hash, String tableName)
        {
            List<String> sqlCommands;

            sqlCommands = new List<String>(hash.Count);
            foreach (KeyValuePair<String, Int32> keyValue in hash)
            {
                StringBuilder insertCommand;

                insertCommand = new StringBuilder();
                insertCommand.Append("INSERT INTO ");
                insertCommand.Append(tableName);
                insertCommand.Append(" VALUES (");
                insertCommand.Append(keyValue.Value.ToString());
                insertCommand.Append(", ");
                insertCommand.Append(PrepareTextForDb(keyValue.Key));
                insertCommand.Append(")");
                sqlCommands.Add(insertCommand.ToString());
            }
            return (sqlCommands);
        }

        private List<String> GetInsertBaseDataCommands<T>(Hashtable<T> hash, String tableName) where T : struct
        {
            List<String> sqlCommands;

            sqlCommands = new List<String>(hash.Count);
            foreach (KeyValuePair<T, Int32> keyValue in hash)
            {
                StringBuilder insertCommand;
                FieldInfo fieldInfo;
                Object[] attributes;
                String name;

                insertCommand = new StringBuilder();
                insertCommand.Append("INSERT INTO ");
                insertCommand.Append(tableName);
                insertCommand.Append(" VALUES (");
                insertCommand.Append(keyValue.Value.ToString());
                insertCommand.Append(", ");
                name = Enum.GetName(typeof(T), keyValue.Key);
                fieldInfo = keyValue.Key.GetType().GetField(name, BindingFlags.Public | BindingFlags.Static);
                attributes = fieldInfo.GetCustomAttributes(false);
                if (attributes != null && attributes.Length > 0)
                {
                    foreach (Object attribute in attributes)
                    {
                        XmlEnumAttribute xmlEnumAttribute;

                        xmlEnumAttribute = attribute as XmlEnumAttribute;
                        if (xmlEnumAttribute != null)
                        {
                            name = xmlEnumAttribute.Name;
                            break;
                        }
                    }
                }
                insertCommand.Append(PrepareTextForDb(name));
                insertCommand.Append(")");
                sqlCommands.Add(insertCommand.ToString());
            }
            return (sqlCommands);
        }

        private List<String> GetInsertBaseDataCommands(PersonHashtable hash, String tableName)
        {
            List<String> sqlCommands;

            sqlCommands = new List<String>(hash.Count);
            foreach (KeyValuePair<PersonKey, Int32> keyValue in hash)
            {
                StringBuilder insertCommand;
                IPerson keyData;

                insertCommand = new StringBuilder();
                insertCommand.Append("INSERT INTO ");
                insertCommand.Append(tableName);
                insertCommand.Append(" VALUES (");
                insertCommand.Append(keyValue.Value.ToString());
                insertCommand.Append(", ");
                keyData = keyValue.Key.KeyData;
                insertCommand.Append(PrepareOptionalTextForDb(keyData.LastName));
                insertCommand.Append(", ");
                insertCommand.Append(PrepareOptionalTextForDb(keyData.MiddleName));
                insertCommand.Append(", ");
                insertCommand.Append(PrepareOptionalTextForDb(keyData.FirstName));
                insertCommand.Append(", ");
                if (keyData.BirthYear == 0)
                {
                    insertCommand.Append(NULL);
                }
                else
                {
                    insertCommand.Append(keyData.BirthYear);
                }
                insertCommand.Append(")");
                sqlCommands.Add(insertCommand.ToString());
            }
            return (sqlCommands);
        }

        private List<String> GetInsertBaseDataCommands(CollectionTypeHashtable hash, String tableName)
        {
            List<String> sqlCommands;

            sqlCommands = new List<String>(hash.Count);
            foreach (KeyValuePair<CollectionType, Int32> keyValue in hash)
            {
                StringBuilder insertCommand;

                insertCommand = new StringBuilder();
                insertCommand.Append("INSERT INTO ");
                insertCommand.Append(tableName);
                insertCommand.Append(" VALUES (");
                insertCommand.Append(keyValue.Value.ToString());
                insertCommand.Append(", ");
                insertCommand.Append(PrepareTextForDb(keyValue.Key.Value));
                insertCommand.Append(", ");
                insertCommand.Append(keyValue.Key.IsPartOfOwnedCollection);
                insertCommand.Append(")");
                sqlCommands.Add(insertCommand.ToString());
            }
            return (sqlCommands);
        }

        private List<String> GetInsertDataCommands()
        {
            if (Collection.DVDList != null && Collection.DVDList.Length > 0)
            {
                List<String> sqlCommands;
                Dictionary<String, Boolean> dvdHash;

                dvdHash = new Dictionary<String, Boolean>(Collection.DVDList.Length);
                sqlCommands = new List<String>(Collection.DVDList.Length * 150);

                foreach (DVD dvd in Collection.DVDList)
                {
                    if (String.IsNullOrEmpty(dvd.ID))
                    {
                        continue;
                    }

                    dvdHash.Add(dvd.ID, true);

                    GetInsertDVDCommand(sqlCommands, dvd);

                    GetInsertDVDIdCommand(sqlCommands, dvd);

                    GetInsertReviewCommand(sqlCommands, dvd);

                    GetInsertLoanInfoCommand(sqlCommands, dvd);

                    GetInsertFeaturesCommand(sqlCommands, dvd);

                    GetInsertFormatCommand(sqlCommands, dvd);

                    GetInsertPurchaseCommand(sqlCommands, dvd);

                    GetInsertLockCommand(sqlCommands, dvd);

                    GetInsertDVDxMediaTypeCommands(sqlCommands, dvd);

                    GetInsertDVDxGenreCommands(sqlCommands, dvd);

                    GetInsertDVDxRegionCommands(sqlCommands, dvd);

                    GetInsertDVDxStudioCommands(sqlCommands, dvd);

                    GetInsertDVDxMediaCompanyCommands(sqlCommands, dvd);

                    GetInsertDVDxAudioCommands(sqlCommands, dvd);

                    GetInsertDVDxSubtitleCommands(sqlCommands, dvd);

                    GetInsertDVDxCastCommands(sqlCommands, dvd);

                    GetInsertDVDxCrewCommands(sqlCommands, dvd);

                    GetInsertDVDxDiscCommands(sqlCommands, dvd);

                    GetInsertDVDxEventCommands(sqlCommands, dvd);

                    GetInsertDVDxTagCommands(sqlCommands, dvd);

                    GetInsertDVDxMyLinksCommands(sqlCommands, dvd);

                    GetInsertDVDxPluginCommands(sqlCommands, dvd);

                    GetInsertDataCommands(sqlCommands, dvd, dvd.CountryOfOrigin);
                    GetInsertDataCommands(sqlCommands, dvd, dvd.CountryOfOrigin2);
                    GetInsertDataCommands(sqlCommands, dvd, dvd.CountryOfOrigin3);
                }

                foreach (DVD dvd in Collection.DVDList)
                {
                    GetUpdateParentDVDIdCommand(sqlCommands, dvdHash, dvd);

                    GetInsertDVDxDVDCommands(sqlCommands, dvdHash, dvd);
                }

                return (sqlCommands);
            }
            else
            {
                return (new List<String>(0));
            }
        }

        private void GetInsertDVDxMediaTypeCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.MediaTypes != null)
            {
                if (dvd.MediaTypes.DVD)
                {
                    GetInsertDVDxMediaTypeDVDCommand(sqlCommands, dvd);
                }
                if (dvd.MediaTypes.BluRay)
                {
                    GetInsertDVDxMediaTypeBlurayCommand(sqlCommands, dvd);
                }
                if (dvd.MediaTypes.HDDVD)
                {
                    GetInsertDVDxMediaTypeHDDVDCommand(sqlCommands, dvd);
                }
                if (String.IsNullOrEmpty(dvd.MediaTypes.CustomMediaType) == false)
                {
                    GetInsertDVDxMediaTypeCustomCommand(sqlCommands, dvd);
                }
            }
        }

        private void GetInsertDVDxDVDCommands(List<String> sqlCommands, Dictionary<String, Boolean> dvdHash, DVD dvd)
        {
            if (dvd.BoxSet.ContentList != null && dvd.BoxSet.ContentList.Length > 0)
            {
                foreach (String dvdId in dvd.BoxSet.ContentList)
                {
                    if (dvdHash.ContainsKey(dvdId))
                    {
                        GetInsertDVDxDVDCommand(sqlCommands, dvd, dvdId);
                    }
                }
            }
        }

        private void GetInsertDVDxMyLinksCommands(List<String> sqlCommands, DVD dvd)
        {
            if ((dvd.MyLinks != null) && (dvd.MyLinks.UserLinkList != null) && (dvd.MyLinks.UserLinkList.Length > 0))
            {
                foreach (UserLink userLink in dvd.MyLinks.UserLinkList)
                {
                    GetInsertDVDxMyLinksCommand(sqlCommands, dvd, userLink);
                }
            }
        }

        private void GetInsertDVDxTagCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.TagList != null && dvd.TagList.Length > 0)
            {
                foreach (Tag tag in dvd.TagList)
                {
                    GetInsertDVDxTagCommand(sqlCommands, dvd, tag);
                }
            }
        }

        private void GetInsertDVDxEventCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.EventList != null && dvd.EventList.Length > 0)
            {
                foreach (Event myEvent in dvd.EventList)
                {
                    GetInsertDVDxEventCommand(sqlCommands, dvd, myEvent);
                }
            }
        }

        private void GetInsertDVDxDiscCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.DiscList != null && dvd.DiscList.Length > 0)
            {
                foreach (Disc disc in dvd.DiscList)
                {
                    GetInsertDVDxDiscCommand(sqlCommands, dvd, disc);
                }
            }
        }

        private void GetInsertDVDxCrewCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.CrewList != null && dvd.CrewList.Length > 0)
            {
                String lastEpisode;
                String lastGroup;
                String lastCreditType;

                lastEpisode = null;
                lastGroup = null;
                lastCreditType = null;
                foreach (Object possibleCrew in dvd.CrewList)
                {
                    CrewMember crew;

                    crew = possibleCrew as CrewMember;
                    if (crew != null)
                    {
                        if (lastCreditType != crew.CreditType)
                        {
                            lastCreditType = crew.CreditType;
                            lastGroup = null;
                        }
                        GetInsertDVDxCrewCommand(sqlCommands, dvd, crew, lastEpisode, lastGroup);
                    }
                    else
                    {
                        GetDividerData(ref lastEpisode, ref lastGroup, (Divider)possibleCrew);
                    }
                }
            }
        }

        private void GetInsertDVDxCastCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.CastList != null && dvd.CastList.Length > 0)
            {
                String lastEpisode;
                String lastGroup;

                lastEpisode = null;
                lastGroup = null;
                foreach (Object possibleCast in dvd.CastList)
                {
                    CastMember cast;

                    cast = possibleCast as CastMember;
                    if (cast != null)
                    {
                        GetInsertDVDxCastCommand(sqlCommands, dvd, cast, lastEpisode, lastGroup);
                    }
                    else
                    {
                        GetDividerData(ref lastEpisode, ref lastGroup, (Divider)possibleCast);
                    }
                }
            }
        }

        private void GetInsertDVDxSubtitleCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.SubtitleList != null && dvd.SubtitleList.Length > 0)
            {
                foreach (String subtitle in dvd.SubtitleList)
                {
                    if (String.IsNullOrEmpty(subtitle) == false)
                    {
                        GetInsertDVDxSubtitleCommand(sqlCommands, dvd, subtitle);
                    }
                }
            }
        }

        private void GetInsertDVDxAudioCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.AudioList != null && dvd.AudioList.Length > 0)
            {
                foreach (AudioTrack audio in dvd.AudioList)
                {
                    GetInsertDVDxAudioCommand(sqlCommands, dvd, audio);
                }
            }
        }

        private void GetInsertDVDxMediaCompanyCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.MediaCompanyList != null && dvd.MediaCompanyList.Length > 0)
            {
                foreach (String distributor in dvd.MediaCompanyList)
                {
                    GetInsertDVDxMediaCompanyCommand(sqlCommands, dvd, distributor);
                }
            }
        }

        private void GetInsertDVDxStudioCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.StudioList != null && dvd.StudioList.Length > 0)
            {
                foreach (String studio in dvd.StudioList)
                {
                    GetInsertDVDxStudioCommand(sqlCommands, dvd, studio);
                }
            }
        }

        private void GetInsertDVDxRegionCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.RegionList != null && dvd.RegionList.Length > 0)
            {
                foreach (String region in dvd.RegionList)
                {
                    GetInsertDVDxRegionCommand(sqlCommands, dvd, region);
                }
            }
        }

        private void GetInsertDVDxPluginCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.PluginCustomData != null && dvd.PluginCustomData.Length > 0)
            {
                foreach (PluginData pluginData in dvd.PluginCustomData)
                {
                    if (pluginData != null)
                    {
                        GetInsertDVDxPluginCommand(sqlCommands, dvd, pluginData);

                        switch (pluginData.ClassID)
                        {
                            case (EPI.ClassGuid.ClassIDBraced):
                                {
                                    GetInsertEnhancedPurchaseInfoCommand(sqlCommands, dvd, pluginData);
                                    break;
                                }
                            case (EN.ClassGuid.ClassIDBraced):
                                {
                                    GetInsertEnhancedNotesCommand(sqlCommands, dvd, pluginData);
                                    break;
                                }
                            case (ET.ClassGuid.ClassIDBraced):
                                {
                                    GetInsertEnhancedTitlesCommand(sqlCommands, dvd, pluginData);
                                    break;
                                }
                        }
                    }
                }
            }
        }

        private void GetInsertEnhancedTitlesCommand(List<String> sqlCommands, DVD dvd, PluginData pluginData)
        {
            if ((pluginData.Any != null) && (pluginData.Any.Length == 1))
            {
                using (StringReader sr = new StringReader(pluginData.Any[0].OuterXml))
                {
                    ET.EnhancedTitles et;

                    et = (ET.EnhancedTitles)(ET.EnhancedTitles.XmlSerializer.Deserialize(sr));
                    GetInsertEnhancedTitlesCommand(sqlCommands, dvd, et);
                }
            }
        }

        private void GetInsertEnhancedTitlesCommand(List<String> sqlCommands, DVD dvd, ET.EnhancedTitles et)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tEnhancedTitles VALUES(");

            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");

            GetEnhancedTitle(insertCommand, et.InternationalEnglishTitle);
            insertCommand.Append(", ");
            GetEnhancedTitle(insertCommand, et.AlternateOriginalTitle);
            insertCommand.Append(", ");
            GetEnhancedTitle(insertCommand, et.NonLatinLettersTitle);
            insertCommand.Append(", ");
            GetEnhancedTitle(insertCommand, et.AdditionalTitle1);
            insertCommand.Append(", ");
            GetEnhancedTitle(insertCommand, et.AdditionalTitle2);

            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetEnhancedTitle(StringBuilder insertCommand, ET.Text text)
        {
            if (text != null)
            {
                String title;

                if (String.IsNullOrEmpty(text.Base64Title))
                {
                    title = text.Value;
                }
                else
                {
                    title = Encoding.UTF8.GetString(Convert.FromBase64String(text.Base64Title));
                }
                insertCommand.Append(PrepareOptionalTextForDb(title));
            }
            else
            {
                insertCommand.Append(NULL);
            }
        }

        private void GetInsertEnhancedNotesCommand(List<String> sqlCommands, DVD dvd, PluginData pluginData)
        {
            if ((pluginData.Any != null) && (pluginData.Any.Length == 1))
            {
                using (StringReader sr = new StringReader(pluginData.Any[0].OuterXml))
                {
                    EN.EnhancedNotes en;

                    en = (EN.EnhancedNotes)(EN.EnhancedNotes.XmlSerializer.Deserialize(sr));
                    GetInsertEnhancedNotesCommand(sqlCommands, dvd, en);
                }
            }
        }

        private void GetInsertEnhancedNotesCommand(List<String> sqlCommands, DVD dvd, EN.EnhancedNotes en)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tEnhancedNotes VALUES(");

            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");

            GetEnhancedNote(insertCommand, en.Note1);
            insertCommand.Append(", ");
            GetEnhancedNote(insertCommand, en.Note2);
            insertCommand.Append(", ");
            GetEnhancedNote(insertCommand, en.Note3);
            insertCommand.Append(", ");
            GetEnhancedNote(insertCommand, en.Note4);
            insertCommand.Append(", ");
            GetEnhancedNote(insertCommand, en.Note5);

            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetEnhancedNote(StringBuilder insertCommand, EN.Text text)
        {
            if (text != null)
            {
                String note;

                if (String.IsNullOrEmpty(text.Base64Note))
                {
                    note = text.Value;
                }
                else
                {
                    note = Encoding.UTF8.GetString(Convert.FromBase64String(text.Base64Note));
                }
                insertCommand.Append(PrepareOptionalTextForDb(note));
                insertCommand.Append(", ");
                insertCommand.Append(text.IsHtml);
            }
            else
            {
                insertCommand.Append(NULL);
                insertCommand.Append(", ");
                insertCommand.Append(NULL);
            }
        }

        private void GetInsertEnhancedPurchaseInfoCommand(List<String> sqlCommands, DVD dvd, PluginData pluginData)
        {
            if ((pluginData.Any != null) && (pluginData.Any.Length == 1))
            {
                using (StringReader sr = new StringReader(pluginData.Any[0].OuterXml))
                {
                    EPI.EnhancedPurchaseInfo epi;

                    epi = (EPI.EnhancedPurchaseInfo)(EPI.EnhancedPurchaseInfo.XmlSerializer.Deserialize(sr));
                    GetInsertEnhancedPurchaseInfoCommand(sqlCommands, dvd, epi);
                }
            }
        }

        private void GetInsertEnhancedPurchaseInfoCommand(List<String> sqlCommands, DVD dvd, EPI.EnhancedPurchaseInfo epi)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tEnhancedPurchaseInfo VALUES(");

            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");

            GetEnhancedPurchaseInfoPrice(insertCommand, epi.OriginalPrice);
            insertCommand.Append(", ");
            GetEnhancedPurchaseInfoPrice(insertCommand, epi.ShippingCost);
            insertCommand.Append(", ");
            GetEnhancedPurchaseInfoPrice(insertCommand, epi.CreditCardCharge);
            insertCommand.Append(", ");
            GetEnhancedPurchaseInfoPrice(insertCommand, epi.CreditCardFees);
            insertCommand.Append(", ");
            GetEnhancedPurchaseInfoPrice(insertCommand, epi.Discount);
            insertCommand.Append(", ");
            GetEnhancedPurchaseInfoPrice(insertCommand, epi.CustomsFees);
            insertCommand.Append(", ");

            GetEnhancedPurchaseInfoText(insertCommand, epi.CouponType);
            insertCommand.Append(", ");
            GetEnhancedPurchaseInfoText(insertCommand, epi.CouponCode);
            insertCommand.Append(", ");

            GetEnhancedPurchaseInfoPrice(insertCommand, epi.AdditionalPrice1);
            insertCommand.Append(", ");
            GetEnhancedPurchaseInfoPrice(insertCommand, epi.AdditionalPrice2);
            insertCommand.Append(", ");

            GetEnhancedPurchaseInfoDate(insertCommand, epi.OrderDate);
            insertCommand.Append(", ");
            GetEnhancedPurchaseInfoDate(insertCommand, epi.ShippingDate);
            insertCommand.Append(", ");
            GetEnhancedPurchaseInfoDate(insertCommand, epi.DeliveryDate);
            insertCommand.Append(", ");
            GetEnhancedPurchaseInfoDate(insertCommand, epi.AdditionalDate1);
            insertCommand.Append(", ");
            GetEnhancedPurchaseInfoDate(insertCommand, epi.AdditionalDate2);

            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetEnhancedPurchaseInfoDate(StringBuilder insertCommand, EPI.Date date)
        {
            if (date != null)
            {
                PrepareDateForDb(insertCommand, date.Value, false);
            }
            else
            {
                insertCommand.Append(NULL);
            }
        }

        private void GetEnhancedPurchaseInfoText(StringBuilder insertCommand, EPI.Text text)
        {
            if (text != null)
            {
                insertCommand.Append(PrepareOptionalTextForDb(text.Value));
            }
            else
            {
                insertCommand.Append(NULL);
            }
        }

        private void GetEnhancedPurchaseInfoPrice(StringBuilder insertCommand, EPI.Price price)
        {
            if (price != null)
            {
                insertCommand.Append(PrepareOptionalTextForDb(price.DenominationType));
                insertCommand.Append(", ");
                insertCommand.Append(price.Value.ToString(CultureInfo.GetCultureInfo("en-US")));
            }
            else
            {
                insertCommand.Append(NULL);
                insertCommand.Append(", ");
                insertCommand.Append(NULL);
            }
        }

        private void GetInsertDVDxGenreCommands(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.GenreList != null && dvd.GenreList.Length > 0)
            {
                foreach (String genre in dvd.GenreList)
                {
                    if (String.IsNullOrEmpty(genre) == false)
                    {
                        GetInsertDVDxGenreCommand(sqlCommands, dvd, genre);
                    }
                }
            }
        }

        private void GetInsertDVDxDVDCommand(List<String> sqlCommands, DVD dvd, String dvdId)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxDVD VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvdId));
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetUpdateParentDVDIdCommand(List<String> sqlCommands, Dictionary<String, Boolean> dvdHash, DVD dvd)
        {
            if (String.IsNullOrEmpty(dvd.BoxSet.Parent) == false && dvdHash.ContainsKey(dvd.BoxSet.Parent))
            {
                StringBuilder updateCommand;

                updateCommand = new StringBuilder();
                updateCommand.Append("UPDATE tDVD SET ParentDVDId = ");
                updateCommand.Append(PrepareTextForDb(dvd.BoxSet.Parent));
                updateCommand.Append(" WHERE Id = ");
                updateCommand.Append(PrepareTextForDb(dvd.ID));
                sqlCommands.Add(updateCommand.ToString());
            }
        }

        private void GetInsertDVDxMyLinksCommand(List<String> sqlCommands, DVD dvd, UserLink userLink)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxMyLinks VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(userLink.URL));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(userLink.Description));
            insertCommand.Append(", ");
            insertCommand.Append(LinkCategoryHash[userLink.Category]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxTagCommand(List<String> sqlCommands, DVD dvd, Tag tag)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxTag VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(TagHash[tag]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxEventCommand(List<String> sqlCommands, DVD dvd, Event myEvent)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxEvent VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(EventTypeHash[myEvent.Type]);
            insertCommand.Append(", ");
            PrepareDateForDb(insertCommand, myEvent.Timestamp, true);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(myEvent.Note));
            insertCommand.Append(", ");
            insertCommand.Append(UserHash[myEvent.User]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxDiscCommand(List<String> sqlCommands, DVD dvd, Disc disc)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxDisc VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(disc.DescriptionSideA));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(disc.DescriptionSideB));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(disc.DiscIDSideA));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(disc.DiscIDSideB));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(disc.LabelSideA));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(disc.LabelSideB));
            insertCommand.Append(", ");
            insertCommand.Append(disc.DualLayeredSideA);
            insertCommand.Append(", ");
            insertCommand.Append(disc.DualLayeredSideB);
            insertCommand.Append(", ");
            insertCommand.Append(disc.DualSided);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(disc.Location));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(disc.Slot));
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxCrewCommand(List<String> sqlCommands, DVD dvd, CrewMember crew, String lastEpisode, String lastGroup)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxCrew VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(CastAndCrewHash[crew]);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(crew.CreditType));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(crew.CreditSubtype));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(crew.CreditedAs));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(lastEpisode));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(lastGroup));
            insertCommand.Append(", ");
            if (crew.CustomRoleSpecified)
            {
                insertCommand.Append(PrepareOptionalTextForDb(crew.CustomRole));
            }
            else
            {
                insertCommand.Append(NULL);
            }
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxCastCommand(List<String> sqlCommands, DVD dvd, CastMember cast, String lastEpisode, String lastGroup)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxCast VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(CastAndCrewHash[cast]);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(cast.Role));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(cast.CreditedAs));
            insertCommand.Append(", ");
            insertCommand.Append(cast.Voice);
            insertCommand.Append(", ");
            insertCommand.Append(cast.Uncredited);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(lastEpisode));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(lastGroup));
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxSubtitleCommand(List<String> sqlCommands, DVD dvd, String subtitle)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxSubtitle VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(SubtitleHash[subtitle]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxAudioCommand(List<String> sqlCommands, DVD dvd, AudioTrack audio)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxAudio VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(AudioContentHash[audio.Content]);
            insertCommand.Append(", ");
            insertCommand.Append(AudioFormatHash[audio.Format]);
            insertCommand.Append(", ");
            insertCommand.Append(AudioChannelsHash[audio.Channels]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaCompanyCommand(List<String> sqlCommands, DVD dvd, String distributor)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxMediaCompany VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(StudioAndMediaCompanyHash[distributor]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxStudioCommand(List<String> sqlCommands, DVD dvd, String studio)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxStudio VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(StudioAndMediaCompanyHash[studio]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxRegionCommand(List<String> sqlCommands, DVD dvd, String region)
        {
            StringBuilder insertCommand;
            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxRegion VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(region));
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxPluginCommand(List<String> sqlCommands, DVD dvd, PluginData pluginData)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxPluginData VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PluginHash[pluginData]);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(GetPluginData(pluginData.Any)));
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private String GetPluginData(XmlNode[] xmlNodes)
        {
            StringBuilder sb;

            if ((xmlNodes == null) || (xmlNodes.Length == 0))
            {
                return (null);
            }
            sb = new StringBuilder();
            foreach (XmlNode xmlNode in xmlNodes)
            {
                if ((xmlNode != null) && (String.IsNullOrEmpty(xmlNode.OuterXml) == false))
                {
                    sb.AppendLine(xmlNode.OuterXml);
                }
            }
            return (sb.ToString());
        }

        private void GetInsertDVDxGenreCommand(List<String> sqlCommands, DVD dvd, String genre)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxGenre VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(GenreHash[genre]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeCustomCommand(List<String> sqlCommands, DVD dvd)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(MediaTypeHash[dvd.MediaTypes.CustomMediaType]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeHDDVDCommand(List<String> sqlCommands, DVD dvd)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(MediaTypeHash["HD-DVD"]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeBlurayCommand(List<String> sqlCommands, DVD dvd)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(MediaTypeHash["Blu-ray"]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeDVDCommand(List<String> sqlCommands, DVD dvd)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(MediaTypeHash["DVD"]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertLockCommand(List<String> sqlCommands, DVD dvd)
        {
            if (dvd.Locks != null)
            {
                StringBuilder insertCommand;

                insertCommand = new StringBuilder();
                insertCommand.Append("INSERT INTO tLock VALUES (");
                insertCommand.Append(PrepareTextForDb(dvd.ID));
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Entire);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Covers);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Title);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.MediaType);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Overview);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Regions);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Genres);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.SRP);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Studios);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.DiscInformation);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Cast);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Crew);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Features);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.AudioTracks);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Subtitles);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.EasterEggs);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.RunningTime);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.ReleaseDate);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.ProductionYear);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.CaseType);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.VideoFormats);
                insertCommand.Append(", ");
                insertCommand.Append(dvd.Locks.Rating);
                insertCommand.Append(")");
                sqlCommands.Add(insertCommand.ToString());
            }
        }

        private void GetInsertPurchaseCommand(List<String> sqlCommands, DVD dvd)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tPurchase VALUES (");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            if (dvd.PurchaseInfo.Price.Value == 0.0f)
            {
                insertCommand.Append(NULL);
                insertCommand.Append(", ");
                insertCommand.Append(NULL);
            }
            else
            {
                insertCommand.Append(PrepareTextForDb(dvd.PurchaseInfo.Price.DenominationType));
                insertCommand.Append(", ");
                insertCommand.Append(dvd.PurchaseInfo.Price.Value.ToString(CultureInfo.GetCultureInfo("en-US")));
            }
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.PurchaseInfo.Place));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.PurchaseInfo.Type));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.PurchaseInfo.Website));
            insertCommand.Append(", ");
            if (dvd.PurchaseInfo.DateSpecified == false)
            {
                insertCommand.Append(NULL);
            }
            else
            {
                PrepareDateForDb(insertCommand, dvd.PurchaseInfo.Date, false);
            }
            insertCommand.Append(", ");
            insertCommand.Append(dvd.PurchaseInfo.ReceivedAsGift);
            insertCommand.Append(", ");
            if (dvd.PurchaseInfo.GiftFrom == null)
            {
                insertCommand.Append(NULL);
            }
            else
            {
                if ((String.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.FirstName) == false)
                    || (String.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.LastName) == false))
                {
                    User user;

                    user = new User(dvd.PurchaseInfo.GiftFrom);
                    insertCommand.Append(UserHash[user]);
                }
                else
                {
                    insertCommand.Append(NULL);
                }
            }
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertFormatCommand(List<String> sqlCommands, DVD dvd)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tFormat VALUES (");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.Format.AspectRatio));
            insertCommand.Append(", ");
            insertCommand.Append(VideoStandardHash[dvd.Format.VideoStandard]);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.LetterBox);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.PanAndScan);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.FullFrame);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.Enhanced16X9);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.DualSided);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.DualLayered);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.Color.Color);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.Color.BlackAndWhite);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.Color.Colorized);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.Color.Mixed);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.Dimensions.Dim2D);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.Dimensions.Dim3DAnaglyph);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.Dimensions.Dim3DBluRay);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertFeaturesCommand(List<String> sqlCommands, DVD dvd)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tFeatures VALUES (");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.SceneAccess);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.Commentary);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.Trailer);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.PhotoGallery);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.DeletedScenes);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.MakingOf);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.ProductionNotes);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.Game);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.DVDROMContent);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.MultiAngle);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.MusicVideos);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.Interviews);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.StoryboardComparisons);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.Outtakes);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.ClosedCaptioned);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.THXCertified);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.Features.OtherFeatures));
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertLoanInfoCommand(List<String> sqlCommands, DVD dvd)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tLoanInfo VALUES (");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(dvd.LoanInfo.Loaned);
            insertCommand.Append(", ");
            if (dvd.LoanInfo.DueSpecified == false)
            {
                insertCommand.Append(NULL);
            }
            else
            {
                PrepareDateForDb(insertCommand, dvd.LoanInfo.Due, false);
            }
            insertCommand.Append(", ");
            if (dvd.LoanInfo.User == null)
            {
                insertCommand.Append(NULL);
            }
            else
            {
                insertCommand.Append(UserHash[dvd.LoanInfo.User]);
            }
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertReviewCommand(List<String> sqlCommands, DVD dvd)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tReview VALUES (");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Review.Film);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Review.Video);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Review.Audio);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Review.Extras);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDIdCommand(List<String> sqlCommands, DVD dvd)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVDId VALUES (");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID_Base));
            insertCommand.Append(", ");
            if (dvd.ID_VariantNum > 0)
            {
                insertCommand.Append(dvd.ID_VariantNum);
            }
            else
            {
                insertCommand.Append(NULL);
            }
            insertCommand.Append(", ");
            if (dvd.ID_LocalityID > 0)
            {
                insertCommand.Append(dvd.ID_LocalityID);
            }
            else
            {
                insertCommand.Append(NULL);
            }
            insertCommand.Append(", ");
            insertCommand.Append(LocalityHash[dvd.ID_LocalityDesc]);
            insertCommand.Append(", ");
            insertCommand.Append(DVDIdTypeHash[dvd.ID_Type]);
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDCommand(List<String> sqlCommands, DVD dvd)
        {
            StringBuilder insertCommand;

            insertCommand = new StringBuilder();
            insertCommand.Append("INSERT INTO tDVD VALUES (");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.UPC));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.CollectionNumber));
            insertCommand.Append(", ");
            insertCommand.Append(CollectionTypeHash[dvd.CollectionType]);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.Title));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.Edition));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.OriginalTitle));
            insertCommand.Append(", ");
            if (dvd.ProductionYear == 0)
            {
                insertCommand.Append(NULL);
            }
            else
            {
                insertCommand.Append(dvd.ProductionYear);
            }
            insertCommand.Append(", ");
            if (dvd.ReleasedSpecified == false)
            {
                insertCommand.Append(NULL);
            }
            else
            {
                PrepareDateForDb(insertCommand, dvd.Released, false);
            }
            insertCommand.Append(", ");
            if (dvd.RunningTime == 0)
            {
                insertCommand.Append(NULL);
            }
            else
            {
                insertCommand.Append(dvd.RunningTime);
            }
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.Rating));
            insertCommand.Append(", ");
            if (String.IsNullOrEmpty(dvd.CaseType) == false)
            {
                insertCommand.Append(CaseTypeHash[dvd.CaseType]);
            }
            else
            {
                insertCommand.Append("NULL");
            }
            insertCommand.Append(", ");
            if (dvd.CaseSlipCoverSpecified == false)
            {
                insertCommand.Append(NULL);
            }
            else
            {
                insertCommand.Append(dvd.CaseSlipCover);
            }
            insertCommand.Append(", ");
            insertCommand.Append(NULL); //BoxSetParent
            insertCommand.Append(", ");
            if (dvd.SRP.Value == 0.0f)
            {
                insertCommand.Append(NULL);
                insertCommand.Append(", ");
                insertCommand.Append(NULL);
            }
            else
            {
                insertCommand.Append(PrepareTextForDb(dvd.SRP.DenominationType));
                insertCommand.Append(", ");
                insertCommand.Append(dvd.SRP.Value.ToString(CultureInfo.GetCultureInfo("en-US")));
            }
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.Overview));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.EasterEggs));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.SortTitle));
            insertCommand.Append(", ");
            PrepareDateForDb(insertCommand, dvd.LastEdited, true);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.WishPriority);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.Notes));
            insertCommand.Append(")");
            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetDividerData(ref String lastEpisode, ref String lastGroup, Divider divider)
        {
            if (divider.Type == DividerType.Episode)
            {
                lastEpisode = divider.Caption;
                lastGroup = null;
            }
            else if (divider.Type == DividerType.Group)
            {
                lastGroup = divider.Caption;
            }
            else
            {
                if (lastGroup != null)
                {
                    lastGroup = null;
                }
                else
                {
                    lastEpisode = null;
                }
            }
        }

        private void GetInsertDataCommands(List<String> sqlCommands, DVD dvd, String countryOfOrigin)
        {
            if (String.IsNullOrEmpty(countryOfOrigin) == false)
            {
                StringBuilder insertCommand;

                insertCommand = new StringBuilder();
                insertCommand.Append("INSERT INTO tDVDxCountryOfOrigin VALUES (");
                insertCommand.Append(IdCounter++);
                insertCommand.Append(", ");
                insertCommand.Append(PrepareTextForDb(dvd.ID));
                insertCommand.Append(", ");
                insertCommand.Append(CountryOfOriginHash[countryOfOrigin]);
                insertCommand.Append(")");
                sqlCommands.Add(insertCommand.ToString());
            }
        }

        private void PrepareDateForDb(StringBuilder insertCommand, DateTime date, Boolean withTime)
        {
            insertCommand.Append("#");
            insertCommand.Append(date.Month);
            insertCommand.Append("/");
            insertCommand.Append(date.Day);
            insertCommand.Append("/");
            insertCommand.Append(date.Year);
            if (withTime)
            {
                insertCommand.Append(" ");
                insertCommand.Append(date.Hour.ToString("00"));
                insertCommand.Append(":");
                insertCommand.Append(date.Minute.ToString("00"));
                insertCommand.Append(":");
                insertCommand.Append(date.Second.ToString("00"));
            }
            insertCommand.Append("#");
        }
        #endregion

        #region Prepare...TextForDb
        private String PrepareTextForDb(String text)
        {
            return ("'" + text.Replace("'", "''") + "'");
        }

        private String PrepareOptionalTextForDb(String text)
        {
            if (String.IsNullOrEmpty(text))
            {
                return (NULL);
            }
            else
            {
                return (PrepareTextForDb(text));
            }
        }
        #endregion

        #region Insert...Data
        private void InsertBaseData()
        {
            List<String> sqlCommands;

            sqlCommands = new List<String>();
            GetInsertUserCommands(sqlCommands);
            InsertData(sqlCommands, "User");

            sqlCommands = new List<String>();
            GetInsertStudioAndMediaCompanyCommands(sqlCommands);
            InsertData(sqlCommands, "StudioAndMediaCompany");

            sqlCommands = new List<String>();
            GetInsertTagCommands(sqlCommands);
            InsertData(sqlCommands, "Tag");

            sqlCommands = new List<String>();
            GetInsertPluginDataCommands(sqlCommands);
            InsertData(sqlCommands, "PluginData");
        }

        private void InsertBaseData(Hashtable<String> hash, String tableName)
        {
            List<String> sqlCommands;

            sqlCommands = GetInsertBaseDataCommands(hash, tableName);
            InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertBaseData<T>(Hashtable<T> hash, String tableName) where T : struct
        {
            List<String> sqlCommands;

            sqlCommands = GetInsertBaseDataCommands(hash, tableName);
            InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertBaseData(CollectionTypeHashtable hash, String tableName)
        {
            List<String> sqlCommands;

            sqlCommands = GetInsertBaseDataCommands(hash, tableName);
            InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertBaseData(PersonHashtable hash, String tableName)
        {
            List<String> sqlCommands;

            sqlCommands = GetInsertBaseDataCommands(hash, tableName);
            InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertData()
        {
            List<String> sqlCommands;

            sqlCommands = GetInsertDataCommands();
            InsertData(sqlCommands, "DVD");
        }

        private void InsertData(List<String> sqlCommands, String section)
        {
            Int32 current;

            if (ProgressMaxChanged != null)
            {
                ProgressMaxChanged(this, new EventArgs<Int32>(sqlCommands.Count));
            }

            if (Feedback != null)
            {
                Feedback(this, new EventArgs<String>(section));
            }

            current = 0;

            foreach (String insertCommand in sqlCommands)
            {
                Command.CommandText = insertCommand;
                try
                {
                    Command.ExecuteNonQuery();
                }
                catch (OleDbException ex)
                {
                    ApplicationException newEx;

                    newEx = new ApplicationException("Error at query:" + Environment.NewLine + insertCommand, ex);
                    throw (newEx);
                }

                if (ProgressValueChanged != null)
                {
                    ProgressValueChanged(this, new EventArgs<Int32>(current));
                }
                current++;
            }

            if (ProgressMaxChanged != null)
            {
                ProgressMaxChanged(this, new EventArgs<Int32>(0));
            }
        }
        #endregion

        private void CheckDBVersion()
        {
            Command.CommandText = "SELECT Version from tDBVersion";
            using (OleDbDataReader reader = Command.ExecuteReader(System.Data.CommandBehavior.SingleRow))
            {
                String version;
                reader.Read();
                version = reader.GetString(0);
                if (version != DVDProfilerSchemaVersion)
                {
                    throw (new InvalidOperationException("Error: Database version incorrect. Abort."));
                }
            }
        }
    }
}
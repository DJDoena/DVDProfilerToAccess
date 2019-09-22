using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using DoenaSoft.DVDProfiler.DVDProfilerHelper;
using DoenaSoft.DVDProfiler.DVDProfilerXML;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    public sealed class SqlProcessor
    {
        #region Fields

        private const string DVDProfilerSchemaVersion = "4.0.0.0";

        internal const string NULL = "NULL";

        private static SqlProcessor s_Instance;

        internal int IdCounter = 1;

        private Hashtable<string> AudioChannelsHash;

        private Hashtable<string> AudioContentHash;

        private Hashtable<string> AudioFormatHash;

        private Hashtable<string> CaseTypeHash;

        private CollectionTypeHashtable CollectionTypeHash;

        private Hashtable<EventType> EventTypeHash;

        private Hashtable<DVDID_Type> DVDIdTypeHash;

        private Hashtable<VideoStandard> VideoStandardHash;

        private Hashtable<string> GenreHash;

        private Hashtable<string> SubtitleHash;

        private Hashtable<string> MediaTypeHash;

        private PersonHashtable CastAndCrewHash;

        private Hashtable<string> StudioAndMediaCompanyHash;

        private TagHashtable TagHash;

        private UserHashtable UserHash;

        private Hashtable<CategoryRestriction> LinkCategoryHash;

        private Hashtable<string> CountryOfOriginHash;

        private Hashtable<string> LocalityHash;

        private PluginHashtable PluginHash;

        private Collection Collection;

        private OleDbCommand Command;

        #endregion

        static SqlProcessor()
        {
            s_Instance = new SqlProcessor();

            FormatInfo = CultureInfo.GetCultureInfo("en-US").NumberFormat;
        }

        private SqlProcessor()
        { }

        public static SqlProcessor Instance
            => (s_Instance);

        internal static NumberFormatInfo FormatInfo { get; }

        public event EventHandler<EventArgs<int>> ProgressMaxChanged;

        public event EventHandler<EventArgs<int>> ProgressValueChanged;

        public event EventHandler<EventArgs<string>> Feedback;

        public ExceptionXml Process(string sourceFile
            , string targetFile)
        {
            OleDbConnection connection = null;

            OleDbTransaction transaction = null;

            ExceptionXml exceptionXml = null;

            try
            {
                //Phase 2: Fill Hashtables
                DVDIdTypeHash = FillStaticHash<DVDID_Type>();

                EventTypeHash = FillStaticHash<EventType>();

                VideoStandardHash = FillStaticHash<VideoStandard>();

                LinkCategoryHash = FillStaticHash<CategoryRestriction>();

                Collection = DVDProfilerSerializer<Collection>.Deserialize(sourceFile);

                FillDynamicHash();

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

                            //Phase 3: Fill Basic Data Into Database
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
                    Feedback?.Invoke(this, new EventArgs<string>($"{Collection.DVDList.Length:#,##0} profiles transformed."));
                }

                Feedback?.Invoke(this, new EventArgs<string>($"{IdCounter:#,##0} database entries created."));
            }
            catch (Exception exception)
            {
                try
                {
                    transaction.Rollback();
                }
                catch
                { }
                try

                {
                    connection.Close();
                }
                catch
                { }

                try
                {
                    if (File.Exists(targetFile))
                    {
                        File.Delete(targetFile);
                    }
                }
                catch
                { }

                Feedback?.Invoke(this, new EventArgs<string>($"Error: {exception.Message} "));

                exceptionXml = new ExceptionXml(exception);
            }

            return (exceptionXml);
        }

        #region Fill...Hash

        private Hashtable<T> FillStaticHash<T>()
            where T : struct
        {
            FieldInfo[] fieldInfos = typeof(T).GetFields(BindingFlags.Public | BindingFlags.Static);

            if (fieldInfos?.Length > 0)
            {
                Hashtable<T> hash = new Hashtable<T>(fieldInfos.Length);

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

            if (Collection.DVDList?.Length > 0)
            {

                foreach (DVD dvd in Collection.DVDList)
                {
                    if (string.IsNullOrEmpty(dvd.ID))
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
            LocalityHash = new Hashtable<string>(5);

            CollectionTypeHash = new CollectionTypeHashtable(5);

            CastAndCrewHash = new PersonHashtable(Collection.DVDList.Length * 50);

            StudioAndMediaCompanyHash = new Hashtable<string>(100);

            AudioChannelsHash = new Hashtable<string>(20);

            AudioContentHash = new Hashtable<string>(20);

            AudioFormatHash = new Hashtable<string>(20);

            CaseTypeHash = new Hashtable<string>(20);

            TagHash = new TagHashtable(50);

            UserHash = new UserHashtable(20);

            GenreHash = new Hashtable<string>(30);

            SubtitleHash = new Hashtable<string>(30);

            MediaTypeHash = new Hashtable<string>(5);

            CountryOfOriginHash = new Hashtable<string>(20);

            PluginHash = new PluginHashtable(5);
        }

        private void FillUserHashFromPurchaseInfo(DVD dvd)
        {
            if (dvd.PurchaseInfo?.GiftFrom != null)
            {
                if ((string.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.FirstName) == false)
                    || (string.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.LastName) == false))
                {
                    User user = new User(dvd.PurchaseInfo.GiftFrom);

                    if (UserHash.ContainsKey(user) == false)
                    {
                        UserHash.Add(user);
                    }
                }
            }
        }

        private void FillPluginHash(DVD dvd)
        {
            if (dvd.PluginCustomData?.Length > 0)
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
            if ((string.IsNullOrEmpty(dvd.CountryOfOrigin) == false) && (CountryOfOriginHash.ContainsKey(dvd.CountryOfOrigin) == false))
            {
                CountryOfOriginHash.Add(dvd.CountryOfOrigin);
            }

            if ((string.IsNullOrEmpty(dvd.CountryOfOrigin2) == false) && (CountryOfOriginHash.ContainsKey(dvd.CountryOfOrigin2) == false))
            {
                CountryOfOriginHash.Add(dvd.CountryOfOrigin2);
            }

            if ((string.IsNullOrEmpty(dvd.CountryOfOrigin3) == false) && (CountryOfOriginHash.ContainsKey(dvd.CountryOfOrigin3) == false))
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

                if ((dvd.MediaTypes.UltraHD) && (MediaTypeHash.ContainsKey("Ultra HD") == false))
                {
                    MediaTypeHash.Add("Ultra HD");
                }

                if ((string.IsNullOrEmpty(dvd.MediaTypes.CustomMediaType) == false)
                    && (MediaTypeHash.ContainsKey(dvd.MediaTypes.CustomMediaType) == false))
                {
                    MediaTypeHash.Add(dvd.MediaTypes.CustomMediaType);
                }
            }
        }

        private void FillSubtitleHash(DVD dvd)
        {
            if (dvd.SubtitleList?.Length > 0)
            {
                foreach (string subtitle in dvd.SubtitleList)
                {
                    if ((string.IsNullOrEmpty(subtitle) == false) && (SubtitleHash.ContainsKey(subtitle) == false))
                    {
                        SubtitleHash.Add(subtitle);
                    }
                }
            }
        }

        private void FillGenreHash(DVD dvd)
        {
            if (dvd.GenreList?.Length > 0)
            {
                foreach (string genre in dvd.GenreList)
                {
                    if ((string.IsNullOrEmpty(genre) == false) && (GenreHash.ContainsKey(genre) == false))
                    {
                        GenreHash.Add(genre);
                    }
                }
            }
        }

        private void FillCaseTypeHash(DVD dvd)
        {
            if (string.IsNullOrEmpty(dvd.CaseType) == false)
            {
                if (CaseTypeHash.ContainsKey(dvd.CaseType) == false)
                {
                    CaseTypeHash.Add(dvd.CaseType);
                }
            }
        }

        private void FillAudioHashes(DVD dvd)
        {
            if (dvd.AudioList?.Length > 0)
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
            if (dvd.TagList?.Length > 0)
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
            if (dvd.MediaCompanyList?.Length > 0)
            {
                foreach (string distributor in dvd.MediaCompanyList)
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
            if (dvd.StudioList?.Length > 0)
            {
                foreach (string studio in dvd.StudioList)
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
            if (dvd.EventList?.Length > 0)
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
            if (dvd.LoanInfo?.User != null)
            {
                if (UserHash.ContainsKey(dvd.LoanInfo.User) == false)
                {
                    UserHash.Add(dvd.LoanInfo.User);
                }
            }
        }

        private void FillCrewHash(DVD dvd)
        {
            if (dvd.CrewList?.Length > 0)
            {
                foreach (object possibleCrew in dvd.CrewList)
                {
                    FillDynamicHash<CrewMember>(CastAndCrewHash, possibleCrew);
                }
            }
        }

        private void FillCastHash(DVD dvd)
        {
            if (dvd.CastList?.Length > 0)
            {
                foreach (object possibleCast in dvd.CastList)
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

        private void FillDynamicHash<T>(PersonHashtable personHash, object possiblePerson)
            where T : class, IPerson
        {
            T person = possiblePerson as T;

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

        private void GetInsertPluginDataCommands(List<string> sqlCommands)
        {
            foreach (KeyValuePair<PluginDataKey, int> keyValue in PluginHash)
            {
                GetInsertPluginDataCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertTagCommands(List<string> sqlCommands)
        {
            foreach (KeyValuePair<TagKey, int> keyValue in TagHash)
            {
                GetInsertTagCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertStudioAndMediaCompanyCommands(List<string> sqlCommands)
        {
            foreach (KeyValuePair<string, int> keyValue in StudioAndMediaCompanyHash)
            {
                GetInsertStudioAndMediaCompanyCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertUserCommands(List<string> sqlCommands)
        {
            foreach (KeyValuePair<UserKey, int> keyValue in UserHash)
            {
                GetInsertUserCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertPluginDataCommand(List<string> sqlCommands
            , KeyValuePair<PluginDataKey, int> keyValue)
        {

            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tPluginData VALUES (");
            insertCommand.Append(keyValue.Value.ToString());
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(keyValue.Key.ClassId.ToString()));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.Name));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertTagCommand(List<string> sqlCommands
            , KeyValuePair<TagKey, int> keyValue)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tTag VALUES (");
            insertCommand.Append(keyValue.Value.ToString());
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.Name));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.FullName));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertStudioAndMediaCompanyCommand(List<string> sqlCommands
            , KeyValuePair<string, int> keyValue)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tStudioAndMediaCompany VALUES (");
            insertCommand.Append(keyValue.Value.ToString());
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertUserCommand(List<string> sqlCommands
            , KeyValuePair<UserKey, int> keyValue)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tUser VALUES (");
            insertCommand.Append(keyValue.Value.ToString());
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.LastName));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.FirstName));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.EmailAddress));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.PhoneNumber));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private List<string> GetInsertBaseDataCommands(Hashtable<string> hash
            , string tableName)
        {
            List<string> sqlCommands = new List<string>(hash.Count);

            foreach (KeyValuePair<string, int> keyValue in hash)
            {
                StringBuilder insertCommand = new StringBuilder();

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

        private List<string> GetInsertBaseDataCommands<T>(Hashtable<T> hash
            , string tableName)
            where T : struct
        {
            List<string> sqlCommands = new List<string>(hash.Count);

            foreach (KeyValuePair<T, int> keyValue in hash)
            {
                StringBuilder insertCommand = new StringBuilder();

                insertCommand.Append("INSERT INTO ");
                insertCommand.Append(tableName);
                insertCommand.Append(" VALUES (");
                insertCommand.Append(keyValue.Value.ToString());
                insertCommand.Append(", ");

                string name = Enum.GetName(typeof(T), keyValue.Key);

                FieldInfo fieldInfo = keyValue.Key.GetType().GetField(name, BindingFlags.Public | BindingFlags.Static);

                object[] attributes = fieldInfo.GetCustomAttributes(false);

                if (attributes?.Length > 0)
                {
                    foreach (object attribute in attributes)
                    {
                        XmlEnumAttribute xmlEnumAttribute = attribute as XmlEnumAttribute;

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

        private List<string> GetInsertBaseDataCommands(PersonHashtable hash
            , string tableName)
        {
            List<string> sqlCommands = new List<string>(hash.Count);

            foreach (KeyValuePair<PersonKey, int> keyValue in hash)
            {
                StringBuilder insertCommand = new StringBuilder();

                insertCommand.Append("INSERT INTO ");
                insertCommand.Append(tableName);
                insertCommand.Append(" VALUES (");
                insertCommand.Append(keyValue.Value.ToString());
                insertCommand.Append(", ");

                var keyData = keyValue.Key;

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

        private List<string> GetInsertBaseDataCommands(CollectionTypeHashtable hash
            , string tableName)
        {
            List<string> sqlCommands = new List<string>(hash.Count);

            foreach (KeyValuePair<CollectionType, int> keyValue in hash)
            {
                StringBuilder insertCommand = new StringBuilder();

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

        private List<string> GetInsertDataCommands()
        {
            if (Collection.DVDList?.Length > 0)
            {
                Dictionary<string, bool> dvdHash = new Dictionary<string, bool>(Collection.DVDList.Length);

                List<string> sqlCommands = new List<string>(Collection.DVDList.Length * 150);

                foreach (DVD dvd in Collection.DVDList)
                {
                    if (string.IsNullOrEmpty(dvd.ID))
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
                return (new List<string>(0));
            }
        }

        private void GetInsertDVDxMediaTypeCommands(List<string> sqlCommands
            , DVD dvd)
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

                if (string.IsNullOrEmpty(dvd.MediaTypes.CustomMediaType) == false)
                {
                    GetInsertDVDxMediaTypeCustomCommand(sqlCommands, dvd);
                }
            }
        }

        private void GetInsertDVDxDVDCommands(List<string> sqlCommands
            , Dictionary<string, bool> dvdHash
            , DVD dvd)
        {
            if (dvd.BoxSet.ContentList?.Length > 0)
            {
                foreach (string dvdId in dvd.BoxSet.ContentList)
                {
                    if (dvdHash.ContainsKey(dvdId))
                    {
                        GetInsertDVDxDVDCommand(sqlCommands, dvd, dvdId);
                    }
                }
            }
        }

        private void GetInsertDVDxMyLinksCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.MyLinks?.UserLinkList?.Length > 0)
            {
                foreach (UserLink userLink in dvd.MyLinks.UserLinkList)
                {
                    GetInsertDVDxMyLinksCommand(sqlCommands, dvd, userLink);
                }
            }
        }

        private void GetInsertDVDxTagCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.TagList?.Length > 0)
            {
                foreach (Tag tag in dvd.TagList)
                {
                    GetInsertDVDxTagCommand(sqlCommands, dvd, tag);
                }
            }
        }

        private void GetInsertDVDxEventCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.EventList?.Length > 0)
            {
                foreach (Event myEvent in dvd.EventList)
                {
                    GetInsertDVDxEventCommand(sqlCommands, dvd, myEvent);
                }
            }
        }

        private void GetInsertDVDxDiscCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.DiscList?.Length > 0)
            {
                foreach (Disc disc in dvd.DiscList)
                {
                    GetInsertDVDxDiscCommand(sqlCommands, dvd, disc);
                }
            }
        }

        private void GetInsertDVDxCrewCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.CrewList?.Length > 0)
            {
                string lastEpisode = null;

                string lastGroup = null;

                string lastCreditType = null;

                foreach (object possibleCrew in dvd.CrewList)
                {
                    CrewMember crew = possibleCrew as CrewMember;

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

        private void GetInsertDVDxCastCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.CastList?.Length > 0)
            {
                string lastEpisode = null;

                string lastGroup = null;

                foreach (object possibleCast in dvd.CastList)
                {
                    CastMember cast = possibleCast as CastMember;

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

        private void GetInsertDVDxSubtitleCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.SubtitleList?.Length > 0)
            {
                foreach (string subtitle in dvd.SubtitleList)
                {
                    if (string.IsNullOrEmpty(subtitle) == false)
                    {
                        GetInsertDVDxSubtitleCommand(sqlCommands, dvd, subtitle);
                    }
                }
            }
        }

        private void GetInsertDVDxAudioCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.AudioList?.Length > 0)
            {
                foreach (AudioTrack audio in dvd.AudioList)
                {
                    GetInsertDVDxAudioCommand(sqlCommands, dvd, audio);
                }
            }
        }

        private void GetInsertDVDxMediaCompanyCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.MediaCompanyList?.Length > 0)
            {
                foreach (string distributor in dvd.MediaCompanyList)
                {
                    GetInsertDVDxMediaCompanyCommand(sqlCommands, dvd, distributor);
                }
            }
        }

        private void GetInsertDVDxStudioCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.StudioList?.Length > 0)
            {
                foreach (string studio in dvd.StudioList)
                {
                    GetInsertDVDxStudioCommand(sqlCommands, dvd, studio);
                }
            }
        }

        private void GetInsertDVDxRegionCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.RegionList?.Length > 0)
            {
                foreach (string region in dvd.RegionList)
                {
                    GetInsertDVDxRegionCommand(sqlCommands, dvd, region);
                }
            }
        }

        private void GetInsertDVDxPluginCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.PluginCustomData?.Length > 0)
            {
                foreach (PluginData pluginData in dvd.PluginCustomData)
                {
                    if (pluginData != null)
                    {
                        GetInsertDVDxPluginCommand(sqlCommands, dvd, pluginData);

                        PluginDataProcessor.GetInsertCommand(sqlCommands, dvd, pluginData);
                    }
                }
            }
        }

        private void GetInsertDVDxGenreCommands(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.GenreList?.Length > 0)
            {
                foreach (string genre in dvd.GenreList)
                {
                    if (string.IsNullOrEmpty(genre) == false)
                    {
                        GetInsertDVDxGenreCommand(sqlCommands, dvd, genre);
                    }
                }
            }
        }

        private void GetInsertDVDxDVDCommand(List<string> sqlCommands
            , DVD dvd
            , string dvdId)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxDVD VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvdId));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetUpdateParentDVDIdCommand(List<string> sqlCommands
            , Dictionary<string, bool> dvdHash
            , DVD dvd)
        {
            if (string.IsNullOrEmpty(dvd.BoxSet.Parent) == false && dvdHash.ContainsKey(dvd.BoxSet.Parent))
            {
                StringBuilder updateCommand = new StringBuilder();

                updateCommand.Append("UPDATE tDVD SET ParentDVDId = ");
                updateCommand.Append(PrepareTextForDb(dvd.BoxSet.Parent));
                updateCommand.Append(" WHERE Id = ");
                updateCommand.Append(PrepareTextForDb(dvd.ID));

                sqlCommands.Add(updateCommand.ToString());
            }
        }

        private void GetInsertDVDxMyLinksCommand(List<string> sqlCommands
            , DVD dvd
            , UserLink userLink)
        {
            StringBuilder insertCommand = new StringBuilder();

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

        private void GetInsertDVDxTagCommand(List<string> sqlCommands
            , DVD dvd
            , Tag tag)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxTag VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(TagHash[tag]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxEventCommand(List<string> sqlCommands
            , DVD dvd
            , Event myEvent)
        {
            StringBuilder insertCommand = new StringBuilder();

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

        private void GetInsertDVDxDiscCommand(List<string> sqlCommands
            , DVD dvd
            , Disc disc)
        {
            StringBuilder insertCommand = new StringBuilder();

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

        private void GetInsertDVDxCrewCommand(List<string> sqlCommands
            , DVD dvd
            , CrewMember crew
            , string lastEpisode
            , string lastGroup)
        {
            StringBuilder insertCommand = new StringBuilder();

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

        private void GetInsertDVDxCastCommand(List<string> sqlCommands
            , DVD dvd
            , CastMember cast
            , string lastEpisode
            , string lastGroup)
        {
            StringBuilder insertCommand = new StringBuilder();

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
            insertCommand.Append(cast.Puppeteer);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(lastEpisode));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(lastGroup));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxSubtitleCommand(List<string> sqlCommands
            , DVD dvd
            , string subtitle)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxSubtitle VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(SubtitleHash[subtitle]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxAudioCommand(List<string> sqlCommands
            , DVD dvd
            , AudioTrack audio)
        {
            StringBuilder insertCommand = new StringBuilder();

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

        private void GetInsertDVDxMediaCompanyCommand(List<string> sqlCommands
            , DVD dvd
            , string distributor)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxMediaCompany VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(StudioAndMediaCompanyHash[distributor]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxStudioCommand(List<string> sqlCommands
            , DVD dvd
            , string studio)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxStudio VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(StudioAndMediaCompanyHash[studio]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxRegionCommand(List<string> sqlCommands
            , DVD dvd
            , string region)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxRegion VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(region));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxPluginCommand(List<string> sqlCommands
            , DVD dvd
            , PluginData pluginData)
        {
            StringBuilder insertCommand = new StringBuilder();

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

        private string GetPluginData(XmlNode[] xmlNodes)
        {
            if ((xmlNodes == null) || (xmlNodes.Length == 0))
            {
                return (null);
            }

            StringBuilder sb = new StringBuilder();

            foreach (XmlNode xmlNode in xmlNodes)
            {
                if ((xmlNode != null) && (string.IsNullOrEmpty(xmlNode.OuterXml) == false))
                {
                    sb.AppendLine(xmlNode.OuterXml);
                }
            }

            return (sb.ToString());
        }

        private void GetInsertDVDxGenreCommand(List<string> sqlCommands
            , DVD dvd
            , string genre)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxGenre VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(GenreHash[genre]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeCustomCommand(List<string> sqlCommands
            , DVD dvd)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(MediaTypeHash[dvd.MediaTypes.CustomMediaType]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeHDDVDCommand(List<string> sqlCommands
            , DVD dvd)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(MediaTypeHash["HD-DVD"]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeBlurayCommand(List<string> sqlCommands
            , DVD dvd)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(MediaTypeHash["Blu-ray"]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeDVDCommand(List<string> sqlCommands
            , DVD dvd)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(MediaTypeHash["DVD"]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertLockCommand(List<string> sqlCommands
            , DVD dvd)
        {
            if (dvd.Locks != null)
            {
                StringBuilder insertCommand = new StringBuilder();

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

        private void GetInsertPurchaseCommand(List<string> sqlCommands
            , DVD dvd)
        {
            StringBuilder insertCommand = new StringBuilder();

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
                insertCommand.Append(dvd.PurchaseInfo.Price.Value.ToString(FormatInfo));
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
                if ((string.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.FirstName) == false)
                    || (string.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.LastName) == false))
                {
                    User user = new User(dvd.PurchaseInfo.GiftFrom);

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

        private void GetInsertFormatCommand(List<string> sqlCommands
            , DVD dvd)
        {
            StringBuilder insertCommand = new StringBuilder();

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
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.DynamicRange?.DRHDR10.ToString() ?? NULL);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Format.DynamicRange?.DRDolbyVision.ToString() ?? NULL);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertFeaturesCommand(List<string> sqlCommands
            , DVD dvd)
        {
            StringBuilder insertCommand = new StringBuilder();

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
            insertCommand.Append(dvd.Features.PIP);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.BDLive);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.BonusTrailers);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.DigitalCopy);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.DBOX);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.CineChat);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.PlayAll);
            insertCommand.Append(", ");
            insertCommand.Append(dvd.Features.MovieIQ);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.Features.OtherFeatures));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertLoanInfoCommand(List<string> sqlCommands
            , DVD dvd)
        {
            StringBuilder insertCommand = new StringBuilder();

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

        private void GetInsertReviewCommand(List<string> sqlCommands
            , DVD dvd)
        {
            StringBuilder insertCommand = new StringBuilder();

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

        private void GetInsertDVDIdCommand(List<string> sqlCommands
            , DVD dvd)
        {
            StringBuilder insertCommand = new StringBuilder();

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

        private void GetInsertDVDCommand(List<string> sqlCommands
            , DVD dvd)
        {
            StringBuilder insertCommand = new StringBuilder();

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

            if (string.IsNullOrEmpty(dvd.CaseType) == false)
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
                insertCommand.Append(dvd.SRP.Value.ToString(FormatInfo));
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

        private void GetDividerData(ref string lastEpisode
            , ref string lastGroup
            , Divider divider)
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

        private void GetInsertDataCommands(List<string> sqlCommands
            , DVD dvd
            , string countryOfOrigin)
        {
            if (string.IsNullOrEmpty(countryOfOrigin) == false)
            {
                StringBuilder insertCommand = new StringBuilder();

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

        internal static void PrepareDateForDb(StringBuilder insertCommand
            , DateTime date
            , bool withTime)
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

        internal static string PrepareTextForDb(string text)
            => ("'" + text.Replace("'", "''") + "'");

        internal static string PrepareOptionalTextForDb(string text)
            => ((string.IsNullOrEmpty(text)) ? NULL : (PrepareTextForDb(text)));

        #endregion

        #region Insert...Data

        private void InsertBaseData()
        {
            List<string> sqlCommands = new List<string>();

            GetInsertUserCommands(sqlCommands);

            InsertData(sqlCommands, "User");

            sqlCommands = new List<string>();

            GetInsertStudioAndMediaCompanyCommands(sqlCommands);

            InsertData(sqlCommands, "StudioAndMediaCompany");

            sqlCommands = new List<string>();

            GetInsertTagCommands(sqlCommands);

            InsertData(sqlCommands, "Tag");

            sqlCommands = new List<string>();

            GetInsertPluginDataCommands(sqlCommands);

            InsertData(sqlCommands, "PluginData");
        }

        private void InsertBaseData(Hashtable<string> hash
            , string tableName)
        {
            List<string> sqlCommands = GetInsertBaseDataCommands(hash, tableName);

            InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertBaseData<T>(Hashtable<T> hash
            , string tableName)
            where T : struct
        {
            List<string> sqlCommands = GetInsertBaseDataCommands(hash, tableName);

            InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertBaseData(CollectionTypeHashtable hash
            , string tableName)
        {
            List<string> sqlCommands = GetInsertBaseDataCommands(hash, tableName);

            InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertBaseData(PersonHashtable hash
            , string tableName)
        {
            List<string> sqlCommands = GetInsertBaseDataCommands(hash, tableName);

            InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertData()
        {
            List<string> sqlCommands = GetInsertDataCommands();

            InsertData(sqlCommands, "DVD");
        }

        private void InsertData(List<string> sqlCommands
            , string section)
        {
            ProgressMaxChanged?.Invoke(this, new EventArgs<int>(sqlCommands.Count));

            Feedback?.Invoke(this, new EventArgs<string>(section));

            int current = 0;

            foreach (string insertCommand in sqlCommands)
            {
                Command.CommandText = insertCommand;

                try
                {
                    Command.ExecuteNonQuery();
                }
                catch (OleDbException ex)
                {
                    throw (new ApplicationException($"Error at query:{Environment.NewLine}{insertCommand}", ex));
                }

                ProgressValueChanged?.Invoke(this, new EventArgs<int>(current));

                current++;
            }

            ProgressMaxChanged?.Invoke(this, new EventArgs<int>(0));
        }

        #endregion

        private void CheckDBVersion()
        {
            Command.CommandText = "SELECT Version from tDBVersion";

            using (OleDbDataReader reader = Command.ExecuteReader(System.Data.CommandBehavior.SingleRow))
            {
                reader.Read();

                string version = reader.GetString(0);

                if (version != DVDProfilerSchemaVersion)
                {
                    throw (new InvalidOperationException("Error: Database version incorrect. Abort."));
                }
            }
        }
    }
}
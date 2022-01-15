namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System;
    using System.Collections.Generic;
    using System.Data.OleDb;
    using System.Globalization;
    using System.IO;
    using System.Reflection;
    using System.Text;
    using System.Xml;
    using System.Xml.Serialization;
    using DVDProfilerHelper;
    using DVDProfilerXML;
    using Profiler = DVDProfilerXML.Version400;

    public sealed class SqlProcessor
    {
        #region Fields

        private const string DVDProfilerSchemaVersion = "4.0.0.0";

        internal const string NULL = "NULL";

        private static readonly SqlProcessor _instance;

        internal int IdCounter { get; set; }

        private Hashtable<string> _audioChannelsHash;

        private Hashtable<string> _audioContentHash;

        private Hashtable<string> _audioFormatHash;

        private Hashtable<string> _caseTypeHash;

        private CollectionTypeHashtable _collectionTypeHash;

        private Hashtable<Profiler.EventType> _eventTypeHash;

        private Hashtable<Profiler.DVDID_Type> _dVDIdTypeHash;

        private Hashtable<Profiler.VideoStandard> _videoStandardHash;

        private Hashtable<string> _genreHash;

        private Hashtable<string> _subtitleHash;

        private Hashtable<string> _mediaTypeHash;

        private PersonHashtable _castAndCrewHash;

        private Hashtable<string> _studioAndMediaCompanyHash;

        private TagHashtable _tagHash;

        private UserHashtable _userHash;

        private Hashtable<Profiler.CategoryRestriction> _linkCategoryHash;

        private Hashtable<string> _countryOfOriginHash;

        private Hashtable<string> _localityHash;

        private PluginHashtable _pluginHash;

        private Profiler.Collection _collection;

        private OleDbCommand _command;

        #endregion

        static SqlProcessor()
        {
            _instance = new SqlProcessor();

            FormatInfo = CultureInfo.GetCultureInfo("en-US").NumberFormat;
        }

        private SqlProcessor()
        {
            this.IdCounter = 1;
        }

        public static SqlProcessor Instance => _instance;

        internal static NumberFormatInfo FormatInfo { get; }

        public event EventHandler<EventArgs<int>> ProgressMaxChanged;

        public event EventHandler<EventArgs<int>> ProgressValueChanged;

        public event EventHandler<EventArgs<string>> Feedback;

        public ExceptionXml Process(string sourceFile, string targetFile)
        {
            try
            {
                //Phase 2: Fill Hashtables
                _dVDIdTypeHash = this.FillStaticHash<Profiler.DVDID_Type>();

                _eventTypeHash = this.FillStaticHash<Profiler.EventType>();

                _videoStandardHash = this.FillStaticHash<Profiler.VideoStandard>();

                _linkCategoryHash = this.FillStaticHash<Profiler.CategoryRestriction>();

                _collection = DVDProfilerSerializer<Profiler.Collection>.Deserialize(sourceFile);

                this.FillDynamicHash();

                if (File.Exists(targetFile))
                {
                    File.Delete(targetFile);
                }

                File.Copy("DVDProfiler.mdb", targetFile);
                File.SetAttributes(targetFile, FileAttributes.Normal | FileAttributes.Archive);
            }
            catch (Exception exception)
            {
                Feedback?.Invoke(this, new EventArgs<string>($"Error: {exception.Message} "));

                var exceptionXml = new ExceptionXml(exception);

                return exceptionXml;
            }

            OleDbConnection connection = null;
            OleDbTransaction transaction = null;
            try
            {
                connection = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + targetFile + ";Persist Security Info=True");

                connection.Open();

                transaction = connection.BeginTransaction();

                using (_command = connection.CreateCommand())
                {
                    _command.Transaction = transaction;

                    this.CheckDBVersion();

                    //Phase 3: Fill Basic Data Into Database
                    this.InsertBaseData(_localityHash, "tLocality");
                    this.InsertBaseData(_dVDIdTypeHash, "tDVDIdType");
                    this.InsertBaseData(_audioChannelsHash, "tAudioChannels");
                    this.InsertBaseData(_audioContentHash, "tAudioContent");
                    this.InsertBaseData(_audioFormatHash, "tAudioFormat");
                    this.InsertBaseData(_caseTypeHash, "tCaseType");
                    this.InsertBaseData(_collectionTypeHash, "tCollectionType");
                    this.InsertBaseData(_eventTypeHash, "tEventType");
                    this.InsertBaseData(_videoStandardHash, "tVideoStandard");
                    this.InsertBaseData(_genreHash, "tGenre");
                    this.InsertBaseData(_subtitleHash, "tSubtitle");
                    this.InsertBaseData(_mediaTypeHash, "tMediaType");
                    this.InsertBaseData(_castAndCrewHash, "tCastAndCrew");
                    this.InsertBaseData(_linkCategoryHash, "tLinkCategory");
                    this.InsertBaseData(_countryOfOriginHash, "tCountryOfOrigin");

                    this.InsertBaseData();

                    //Phase 4: Fill Profiler.DVDs into Database
                    this.InsertData();
                }

                //Phase 5: Save & Exit
                transaction.Commit();

                connection.Close();

                transaction.Dispose();

                connection.Dispose();
            }
            catch (Exception exception)
            {
                try
                {
                    transaction?.Rollback();
                }
                catch
                { }

                try

                {
                    connection?.Close();
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

                var exceptionXml = new ExceptionXml(exception);

                return exceptionXml;
            }

            return null;
        }

        #region Fill...Hash

        private Hashtable<T> FillStaticHash<T>() where T : struct
        {
            var fieldInfos = typeof(T).GetFields(BindingFlags.Public | BindingFlags.Static);

            if (fieldInfos?.Length > 0)
            {
                Hashtable<T> hash = new Hashtable<T>(fieldInfos.Length);

                foreach (FieldInfo fieldInfo in fieldInfos)
                {
                    hash.Add((T)fieldInfo.GetRawConstantValue());
                }

                return hash;
            }
            else
            {
                return new Hashtable<T>(0);
            }
        }

        private void FillDynamicHash()
        {
            this.InitializeHashes();

            if (_collection.DVDList?.Length > 0)
            {

                foreach (var dvd in _collection.DVDList)
                {
                    if (string.IsNullOrEmpty(dvd.ID))
                    {
                        continue;
                    }

                    this.FillLocalityHash(dvd);

                    this.FillCollectionTypeHash(dvd);

                    this.FillCastHash(dvd);

                    this.FillCrewHash(dvd);

                    this.FillUserHashFromLoanInfo(dvd);

                    this.FillUserHashFromEvents(dvd);

                    this.FillStudioHash(dvd);

                    this.FillMediaCompanyHash(dvd);

                    this.FillTagHash(dvd);

                    this.FillAudioHashes(dvd);

                    this.FillCaseTypeHash(dvd);

                    this.FillGenreHash(dvd);

                    this.FillSubtitleHash(dvd);

                    this.FillMediaTypeHash(dvd);

                    this.FillCountryOfOriginHash(dvd);

                    this.FillPluginHash(dvd);
                }

                foreach (var dvd in _collection.DVDList)
                {
                    //second iteration for data that is less complete
                    this.FillUserHashFromPurchaseInfo(dvd);
                }
            }
        }

        private void InitializeHashes()
        {
            _localityHash = new Hashtable<string>(5);

            _collectionTypeHash = new CollectionTypeHashtable(5);

            _castAndCrewHash = new PersonHashtable(_collection.DVDList.Length * 50);

            _studioAndMediaCompanyHash = new Hashtable<string>(100);

            _audioChannelsHash = new Hashtable<string>(20);

            _audioContentHash = new Hashtable<string>(20);

            _audioFormatHash = new Hashtable<string>(20);

            _caseTypeHash = new Hashtable<string>(20);

            _tagHash = new TagHashtable(50);

            _userHash = new UserHashtable(20);

            _genreHash = new Hashtable<string>(30);

            _subtitleHash = new Hashtable<string>(30);

            _mediaTypeHash = new Hashtable<string>(5);

            _countryOfOriginHash = new Hashtable<string>(20);

            _pluginHash = new PluginHashtable(5);
        }

        private void FillUserHashFromPurchaseInfo(Profiler.DVD dvd)
        {
            if (dvd.PurchaseInfo?.GiftFrom != null)
            {
                if (!string.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.FirstName)
                    || !string.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.LastName))
                {
                    var user = new Profiler.User(dvd.PurchaseInfo.GiftFrom);

                    if (!_userHash.ContainsKey(user))
                    {
                        _userHash.Add(user);
                    }
                }
            }
        }

        private void FillPluginHash(Profiler.DVD dvd)
        {
            if (dvd.PluginCustomData?.Length > 0)
            {
                foreach (var pluginData in dvd.PluginCustomData)
                {
                    if (pluginData != null && !_pluginHash.ContainsKey(pluginData))
                    {
                        _pluginHash.Add(pluginData);
                    }
                }
            }
        }

        private void FillCountryOfOriginHash(Profiler.DVD dvd)
        {
            if (!string.IsNullOrEmpty(dvd.CountryOfOrigin) && !_countryOfOriginHash.ContainsKey(dvd.CountryOfOrigin))
            {
                _countryOfOriginHash.Add(dvd.CountryOfOrigin);
            }

            if (!string.IsNullOrEmpty(dvd.CountryOfOrigin2) && !_countryOfOriginHash.ContainsKey(dvd.CountryOfOrigin2))
            {
                _countryOfOriginHash.Add(dvd.CountryOfOrigin2);
            }

            if (!string.IsNullOrEmpty(dvd.CountryOfOrigin3) && !_countryOfOriginHash.ContainsKey(dvd.CountryOfOrigin3))
            {
                _countryOfOriginHash.Add(dvd.CountryOfOrigin3);
            }
        }

        private void FillMediaTypeHash(Profiler.DVD dvd)
        {
            if (dvd.MediaTypes != null)
            {
                if (dvd.MediaTypes.DVD && !_mediaTypeHash.ContainsKey("DVD"))
                {
                    _mediaTypeHash.Add("DVD");
                }

                if (dvd.MediaTypes.BluRay && !_mediaTypeHash.ContainsKey("Blu-ray"))
                {
                    _mediaTypeHash.Add("Blu-ray");
                }

                if (dvd.MediaTypes.HDDVD && !_mediaTypeHash.ContainsKey("HD-DVD"))
                {
                    _mediaTypeHash.Add("HD-DVD");
                }

                if (dvd.MediaTypes.UltraHD && !_mediaTypeHash.ContainsKey("Ultra HD"))
                {
                    _mediaTypeHash.Add("Ultra HD");
                }

                if (!string.IsNullOrEmpty(dvd.MediaTypes.CustomMediaType)
                    && !_mediaTypeHash.ContainsKey(dvd.MediaTypes.CustomMediaType))
                {
                    _mediaTypeHash.Add(dvd.MediaTypes.CustomMediaType);
                }
            }
        }

        private void FillSubtitleHash(Profiler.DVD dvd)
        {
            if (dvd.SubtitleList?.Length > 0)
            {
                foreach (var subtitle in dvd.SubtitleList)
                {
                    if (!string.IsNullOrEmpty(subtitle) && !_subtitleHash.ContainsKey(subtitle))
                    {
                        _subtitleHash.Add(subtitle);
                    }
                }
            }
        }

        private void FillGenreHash(Profiler.DVD dvd)
        {
            if (dvd.GenreList?.Length > 0)
            {
                foreach (var genre in dvd.GenreList)
                {
                    if (!string.IsNullOrEmpty(genre) && !_genreHash.ContainsKey(genre))
                    {
                        _genreHash.Add(genre);
                    }
                }
            }
        }

        private void FillCaseTypeHash(Profiler.DVD dvd)
        {
            if (!string.IsNullOrEmpty(dvd.CaseType))
            {
                if (!_caseTypeHash.ContainsKey(dvd.CaseType))
                {
                    _caseTypeHash.Add(dvd.CaseType);
                }
            }
        }

        private void FillAudioHashes(Profiler.DVD dvd)
        {
            if (dvd.AudioList?.Length > 0)
            {
                foreach (var audioTrack in dvd.AudioList)
                {
                    if (!_audioContentHash.ContainsKey(audioTrack.Content))
                    {
                        _audioContentHash.Add(audioTrack.Content);
                    }

                    if (!_audioFormatHash.ContainsKey(audioTrack.Format))
                    {
                        _audioFormatHash.Add(audioTrack.Format);
                    }

                    if (!_audioChannelsHash.ContainsKey(audioTrack.Channels))
                    {
                        _audioChannelsHash.Add(audioTrack.Channels);
                    }

                }
            }
        }

        private void FillTagHash(Profiler.DVD dvd)
        {
            if (dvd.TagList?.Length > 0)
            {
                foreach (var tag in dvd.TagList)
                {
                    if (_tagHash.ContainsKey(tag) == false)
                    {
                        _tagHash.Add(tag);
                    }
                }
            }
        }

        private void FillMediaCompanyHash(Profiler.DVD dvd)
        {
            if (dvd.MediaCompanyList?.Length > 0)
            {
                foreach (var distributor in dvd.MediaCompanyList)
                {
                    if (!_studioAndMediaCompanyHash.ContainsKey(distributor))
                    {
                        _studioAndMediaCompanyHash.Add(distributor);
                    }
                }
            }
        }

        private void FillStudioHash(Profiler.DVD dvd)
        {
            if (dvd.StudioList?.Length > 0)
            {
                foreach (var studio in dvd.StudioList)
                {
                    if (!_studioAndMediaCompanyHash.ContainsKey(studio))
                    {
                        _studioAndMediaCompanyHash.Add(studio);
                    }
                }
            }
        }

        private void FillUserHashFromEvents(Profiler.DVD dvd)
        {
            if (dvd.EventList?.Length > 0)
            {
                foreach (var myEvent in dvd.EventList)
                {
                    if (!_userHash.ContainsKey(myEvent.User))
                    {
                        _userHash.Add(myEvent.User);
                    }
                }
            }
        }

        private void FillUserHashFromLoanInfo(Profiler.DVD dvd)
        {
            if (dvd.LoanInfo?.User != null)
            {
                if (!_userHash.ContainsKey(dvd.LoanInfo.User))
                {
                    _userHash.Add(dvd.LoanInfo.User);
                }
            }
        }

        private void FillCrewHash(Profiler.DVD dvd)
        {
            if (dvd.CrewList?.Length > 0)
            {
                foreach (var possibleCrew in dvd.CrewList)
                {
                    this.FillDynamicHash<Profiler.CrewMember>(_castAndCrewHash, possibleCrew);
                }
            }
        }

        private void FillCastHash(Profiler.DVD dvd)
        {
            if (dvd.CastList?.Length > 0)
            {
                foreach (var possibleCast in dvd.CastList)
                {
                    this.FillDynamicHash<Profiler.CastMember>(_castAndCrewHash, possibleCast);
                }
            }
        }

        private void FillCollectionTypeHash(Profiler.DVD dvd)
        {
            if (!_collectionTypeHash.ContainsKey(dvd.CollectionType))
            {
                _collectionTypeHash.Add(dvd.CollectionType);
            }
        }

        private void FillLocalityHash(Profiler.DVD dvd)
        {
            if (!_localityHash.ContainsKey(dvd.ID_LocalityDesc))
            {
                _localityHash.Add(dvd.ID_LocalityDesc);
            }
        }

        private void FillDynamicHash<T>(PersonHashtable personHash, object possiblePerson) where T : class, IPerson
        {
            if (possiblePerson is T person)
            {
                if (!personHash.ContainsKey(person))
                {
                    personHash.Add(person);
                }
            }
        }

        #endregion

        #region GetInsert...Command(s)

        private void GetInsertPluginDataCommands(List<string> sqlCommands)
        {
            foreach (var keyValue in _pluginHash)
            {
                this.GetInsertPluginDataCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertTagCommands(List<string> sqlCommands)
        {
            foreach (var keyValue in _tagHash)
            {
                this.GetInsertTagCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertStudioAndMediaCompanyCommands(List<string> sqlCommands)
        {
            foreach (var keyValue in _studioAndMediaCompanyHash)
            {
                this.GetInsertStudioAndMediaCompanyCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertUserCommands(List<string> sqlCommands)
        {
            foreach (var keyValue in _userHash)
            {
                this.GetInsertUserCommand(sqlCommands, keyValue);
            }
        }

        private void GetInsertPluginDataCommand(List<string> sqlCommands, KeyValuePair<PluginDataKey, int> keyValue)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tPluginData VALUES (");
            insertCommand.Append(keyValue.Value.ToString());
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(keyValue.Key.ClassId.ToString()));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.Name));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertTagCommand(List<string> sqlCommands, KeyValuePair<TagKey, int> keyValue)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tTag VALUES (");
            insertCommand.Append(keyValue.Value.ToString());
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.Name));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key.FullName));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertStudioAndMediaCompanyCommand(List<string> sqlCommands, KeyValuePair<string, int> keyValue)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tStudioAndMediaCompany VALUES (");
            insertCommand.Append(keyValue.Value.ToString());
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(keyValue.Key));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertUserCommand(List<string> sqlCommands, KeyValuePair<UserKey, int> keyValue)
        {
            var insertCommand = new StringBuilder();

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

        private List<string> GetInsertBaseDataCommands(Hashtable<string> hash, string tableName)
        {
            var sqlCommands = new List<string>(hash.Count);

            foreach (var keyValue in hash)
            {
                var insertCommand = new StringBuilder();

                insertCommand.Append("INSERT INTO ");
                insertCommand.Append(tableName);
                insertCommand.Append(" VALUES (");
                insertCommand.Append(keyValue.Value.ToString());
                insertCommand.Append(", ");
                insertCommand.Append(PrepareTextForDb(keyValue.Key));
                insertCommand.Append(")");

                sqlCommands.Add(insertCommand.ToString());
            }

            return sqlCommands;
        }

        private List<string> GetInsertBaseDataCommands<T>(Hashtable<T> hash, string tableName) where T : struct
        {
            var sqlCommands = new List<string>(hash.Count);

            foreach (var keyValue in hash)
            {
                var insertCommand = new StringBuilder();

                insertCommand.Append("INSERT INTO ");
                insertCommand.Append(tableName);
                insertCommand.Append(" VALUES (");
                insertCommand.Append(keyValue.Value.ToString());
                insertCommand.Append(", ");

                string name = Enum.GetName(typeof(T), keyValue.Key);

                var fieldInfo = keyValue.Key.GetType().GetField(name, BindingFlags.Public | BindingFlags.Static);

                var attributes = fieldInfo.GetCustomAttributes(false);

                if (attributes?.Length > 0)
                {
                    foreach (var attribute in attributes)
                    {
                        if (attribute is XmlEnumAttribute xmlEnumAttribute)
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

            return sqlCommands;
        }

        private List<string> GetInsertBaseDataCommands(PersonHashtable hash, string tableName)
        {
            var sqlCommands = new List<string>(hash.Count);

            foreach (var keyValue in hash)
            {
                var insertCommand = new StringBuilder();

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

            return sqlCommands;
        }

        private List<string> GetInsertBaseDataCommands(CollectionTypeHashtable hash, string tableName)
        {
            var sqlCommands = new List<string>(hash.Count);

            foreach (var keyValue in hash)
            {
                var insertCommand = new StringBuilder();

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

            return sqlCommands;
        }

        private IEnumerable<Dictionary<string, List<string>>> GetInsertDataCommands()
        {
            if (_collection.DVDList?.Length > 0)
            {
                var dvdHash = new Dictionary<string, bool>(_collection.DVDList.Length);

                var commandGroups = new Dictionary<string, List<string>>()
                {
                    { "DVD", new List<string>() },
                    { "DVDId", new List<string>() },
                    { "Review", new List<string>() },
                    { "LoanInfo", new List<string>() },
                    { "Features", new List<string>() },
                    { "Format", new List<string>() },
                    { "Purchase", new List<string>() },
                    { "Lock", new List<string>() },
                    { "DVDxMediaType", new List<string>() },
                    { "DVDxGenre", new List<string>() },
                    { "DVDxRegion", new List<string>() },
                    { "DVDxStudio", new List<string>() },
                    { "DVDxMediaCompany", new List<string>() },
                    { "DVDxAudio", new List<string>() },
                    { "DVDxSubtitle", new List<string>() },
                    { "DVDxCast", new List<string>() },
                    { "DVDxCrew", new List<string>() },
                    { "DVDxDisc", new List<string>() },
                    { "DVDxEvent", new List<string>() },
                    { "DVDxTag", new List<string>() },
                    { "DVDxMyLinks", new List<string>() },
                    { "DVDxPlugin", new List<string>() },
                    { "DVDxCountryOfOrigin", new List<string>() },
                };

                foreach (var dvd in _collection.DVDList)
                {
                    if (string.IsNullOrEmpty(dvd.ID))
                    {
                        continue;
                    }

                    dvdHash.Add(dvd.ID, true);

                    this.GetInsertDVDCommand(commandGroups["DVD"], dvd);

                    this.GetInsertDVDIdCommand(commandGroups["DVDId"], dvd);

                    this.GetInsertReviewCommand(commandGroups["Review"], dvd);

                    this.GetInsertLoanInfoCommand(commandGroups["LoanInfo"], dvd);

                    this.GetInsertFeaturesCommand(commandGroups["Features"], dvd);

                    this.GetInsertFormatCommand(commandGroups["Format"], dvd);

                    this.GetInsertPurchaseCommand(commandGroups["Purchase"], dvd);

                    this.GetInsertLockCommand(commandGroups["Lock"], dvd);

                    this.GetInsertDVDxMediaTypeCommands(commandGroups["DVDxMediaType"], dvd);

                    this.GetInsertDVDxGenreCommands(commandGroups["DVDxGenre"], dvd);

                    this.GetInsertDVDxRegionCommands(commandGroups["DVDxRegion"], dvd);

                    this.GetInsertDVDxStudioCommands(commandGroups["DVDxStudio"], dvd);

                    this.GetInsertDVDxMediaCompanyCommands(commandGroups["DVDxMediaCompany"], dvd);

                    this.GetInsertDVDxAudioCommands(commandGroups["DVDxAudio"], dvd);

                    this.GetInsertDVDxSubtitleCommands(commandGroups["DVDxSubtitle"], dvd);

                    this.GetInsertDVDxCastCommands(commandGroups["DVDxCast"], dvd);

                    this.GetInsertDVDxCrewCommands(commandGroups["DVDxCrew"], dvd);

                    this.GetInsertDVDxDiscCommands(commandGroups["DVDxDisc"], dvd);

                    this.GetInsertDVDxEventCommands(commandGroups["DVDxEvent"], dvd);

                    this.GetInsertDVDxTagCommands(commandGroups["DVDxTag"], dvd);

                    this.GetInsertDVDxMyLinksCommands(commandGroups["DVDxMyLinks"], dvd);

                    this.GetInsertDVDxPluginCommands(commandGroups["DVDxPlugin"], dvd);

                    this.GetInsertDataCommands(commandGroups["DVDxCountryOfOrigin"], dvd, dvd.CountryOfOrigin);
                    this.GetInsertDataCommands(commandGroups["DVDxCountryOfOrigin"], dvd, dvd.CountryOfOrigin2);
                    this.GetInsertDataCommands(commandGroups["DVDxCountryOfOrigin"], dvd, dvd.CountryOfOrigin3);
                }

                yield return commandGroups;

                commandGroups = new Dictionary<string, List<string>>()
                {
                    { "BoxSetParent", new List<string>(_collection.DVDList.Length) },
                    { "BoxSetChildren", new List<string>(_collection.DVDList.Length) },
                };

                foreach (Profiler.DVD dvd in _collection.DVDList)
                {
                    this.GetUpdateParentDVDIdCommand(commandGroups["BoxSetParent"], dvdHash, dvd);

                    this.GetInsertDVDxDVDCommands(commandGroups["BoxSetChildren"], dvdHash, dvd);
                }

                yield return commandGroups;
            }
        }

        private void GetInsertDVDxMediaTypeCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.MediaTypes != null)
            {
                if (dvd.MediaTypes.DVD)
                {
                    this.GetInsertDVDxMediaTypeDVDCommand(sqlCommands, dvd);
                }

                if (dvd.MediaTypes.BluRay)
                {
                    this.GetInsertDVDxMediaTypeBlurayCommand(sqlCommands, dvd);
                }

                if (dvd.MediaTypes.HDDVD)
                {
                    this.GetInsertDVDxMediaTypeHDDVDCommand(sqlCommands, dvd);
                }

                if (!string.IsNullOrEmpty(dvd.MediaTypes.CustomMediaType))
                {
                    this.GetInsertDVDxMediaTypeCustomCommand(sqlCommands, dvd);
                }
            }
        }

        private void GetInsertDVDxDVDCommands(List<string> sqlCommands, Dictionary<string, bool> dvdHash, Profiler.DVD dvd)
        {
            if (dvd.BoxSet.ContentList?.Length > 0)
            {
                foreach (var dvdId in dvd.BoxSet.ContentList)
                {
                    if (dvdHash.ContainsKey(dvdId))
                    {
                        this.GetInsertDVDxDVDCommand(sqlCommands, dvd, dvdId);
                    }
                }
            }
        }

        private void GetInsertDVDxMyLinksCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.MyLinks?.UserLinkList?.Length > 0)
            {
                foreach (var userLink in dvd.MyLinks.UserLinkList)
                {
                    this.GetInsertDVDxMyLinksCommand(sqlCommands, dvd, userLink);
                }
            }
        }

        private void GetInsertDVDxTagCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.TagList?.Length > 0)
            {
                foreach (var tag in dvd.TagList)
                {
                    this.GetInsertDVDxTagCommand(sqlCommands, dvd, tag);
                }
            }
        }

        private void GetInsertDVDxEventCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.EventList?.Length > 0)
            {
                foreach (var myEvent in dvd.EventList)
                {
                    this.GetInsertDVDxEventCommand(sqlCommands, dvd, myEvent);
                }
            }
        }

        private void GetInsertDVDxDiscCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.DiscList?.Length > 0)
            {
                foreach (var disc in dvd.DiscList)
                {
                    this.GetInsertDVDxDiscCommand(sqlCommands, dvd, disc);
                }
            }
        }

        private void GetInsertDVDxCrewCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.CrewList?.Length > 0)
            {
                string lastEpisode = null;

                string lastGroup = null;

                string lastCreditType = null;

                foreach (object possibleCrew in dvd.CrewList)
                {
                    if (possibleCrew is Profiler.CrewMember crew)
                    {
                        if (lastCreditType != crew.CreditType)
                        {
                            lastCreditType = crew.CreditType;

                            lastGroup = null;
                        }

                        this.GetInsertDVDxCrewCommand(sqlCommands, dvd, crew, lastEpisode, lastGroup);
                    }
                    else
                    {
                        this.GetDividerData(ref lastEpisode, ref lastGroup, (Profiler.Divider)possibleCrew);
                    }
                }
            }
        }

        private void GetInsertDVDxCastCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.CastList?.Length > 0)
            {
                string lastEpisode = null;

                string lastGroup = null;

                foreach (object possibleCast in dvd.CastList)
                {
                    if (possibleCast is Profiler.CastMember cast)
                    {
                        this.GetInsertDVDxCastCommand(sqlCommands, dvd, cast, lastEpisode, lastGroup);
                    }
                    else
                    {
                        this.GetDividerData(ref lastEpisode, ref lastGroup, (Profiler.Divider)possibleCast);
                    }
                }
            }
        }

        private void GetInsertDVDxSubtitleCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.SubtitleList?.Length > 0)
            {
                foreach (var subtitle in dvd.SubtitleList)
                {
                    if (string.IsNullOrEmpty(subtitle) == false)
                    {
                        this.GetInsertDVDxSubtitleCommand(sqlCommands, dvd, subtitle);
                    }
                }
            }
        }

        private void GetInsertDVDxAudioCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.AudioList?.Length > 0)
            {
                foreach (var audio in dvd.AudioList)
                {
                    this.GetInsertDVDxAudioCommand(sqlCommands, dvd, audio);
                }
            }
        }

        private void GetInsertDVDxMediaCompanyCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.MediaCompanyList?.Length > 0)
            {
                foreach (var distributor in dvd.MediaCompanyList)
                {
                    this.GetInsertDVDxMediaCompanyCommand(sqlCommands, dvd, distributor);
                }
            }
        }

        private void GetInsertDVDxStudioCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.StudioList?.Length > 0)
            {
                foreach (var studio in dvd.StudioList)
                {
                    this.GetInsertDVDxStudioCommand(sqlCommands, dvd, studio);
                }
            }
        }

        private void GetInsertDVDxRegionCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.RegionList?.Length > 0)
            {
                foreach (var region in dvd.RegionList)
                {
                    this.GetInsertDVDxRegionCommand(sqlCommands, dvd, region);
                }
            }
        }

        private void GetInsertDVDxPluginCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.PluginCustomData?.Length > 0)
            {
                foreach (var pluginData in dvd.PluginCustomData)
                {
                    if (pluginData != null)
                    {
                        this.GetInsertDVDxPluginCommand(sqlCommands, dvd, pluginData);

                        PluginDataProcessor.GetInsertCommand(sqlCommands, dvd, pluginData);
                    }
                }
            }
        }

        private void GetInsertDVDxGenreCommands(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.GenreList?.Length > 0)
            {
                foreach (var genre in dvd.GenreList)
                {
                    if (!string.IsNullOrEmpty(genre))
                    {
                        this.GetInsertDVDxGenreCommand(sqlCommands, dvd, genre);
                    }
                }
            }
        }

        private void GetInsertDVDxDVDCommand(List<string> sqlCommands, Profiler.DVD dvd, string dvdId)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxDVD VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvdId));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetUpdateParentDVDIdCommand(List<string> sqlCommands, Dictionary<string, bool> dvdHash, Profiler.DVD dvd)
        {
            if (!string.IsNullOrEmpty(dvd.BoxSet.Parent) && dvdHash.ContainsKey(dvd.BoxSet.Parent))
            {
                var updateCommand = new StringBuilder();

                updateCommand.Append("UPDATE tDVD SET ParentDVDId = ");
                updateCommand.Append(PrepareTextForDb(dvd.BoxSet.Parent));
                updateCommand.Append(" WHERE Id = ");
                updateCommand.Append(PrepareTextForDb(dvd.ID));

                sqlCommands.Add(updateCommand.ToString());
            }
        }

        private void GetInsertDVDxMyLinksCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.UserLink userLink)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxMyLinks VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(userLink.URL));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(userLink.Description));
            insertCommand.Append(", ");
            insertCommand.Append(_linkCategoryHash[userLink.Category]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxTagCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.Tag tag)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxTag VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_tagHash[tag]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxEventCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.Event myEvent)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxEvent VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_eventTypeHash[myEvent.Type]);
            insertCommand.Append(", ");

            PrepareDateForDb(insertCommand, myEvent.Timestamp, true);

            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(myEvent.Note));
            insertCommand.Append(", ");
            insertCommand.Append(_userHash[myEvent.User]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxDiscCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.Disc disc)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxDisc VALUES (");
            insertCommand.Append(this.IdCounter++);
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

        private void GetInsertDVDxCrewCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.CrewMember crew, string lastEpisode
            , string lastGroup)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxCrew VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_castAndCrewHash[crew]);
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

        private void GetInsertDVDxCastCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.CastMember cast, string lastEpisode, string lastGroup)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxCast VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_castAndCrewHash[cast]);
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

        private void GetInsertDVDxSubtitleCommand(List<string> sqlCommands, Profiler.DVD dvd, string subtitle)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxSubtitle VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_subtitleHash[subtitle]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxAudioCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.AudioTrack audio)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxAudio VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_audioContentHash[audio.Content]);
            insertCommand.Append(", ");
            insertCommand.Append(_audioFormatHash[audio.Format]);
            insertCommand.Append(", ");
            insertCommand.Append(_audioChannelsHash[audio.Channels]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaCompanyCommand(List<string> sqlCommands, Profiler.DVD dvd, string distributor)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxMediaCompany VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_studioAndMediaCompanyHash[distributor]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxStudioCommand(List<string> sqlCommands, Profiler.DVD dvd, string studio)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxStudio VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_studioAndMediaCompanyHash[studio]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxRegionCommand(List<string> sqlCommands, Profiler.DVD dvd, string region)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxRegion VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(region));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxPluginCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.PluginData pluginData)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxPluginData VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_pluginHash[pluginData]);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(this.GetPluginData(pluginData.Any)));
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private string GetPluginData(XmlNode[] xmlNodes)
        {
            if ((xmlNodes == null) || (xmlNodes.Length == 0))
            {
                return null;
            }

            var sb = new StringBuilder();

            foreach (var xmlNode in xmlNodes)
            {
                if ((xmlNode != null) && (string.IsNullOrEmpty(xmlNode.OuterXml) == false))
                {
                    sb.AppendLine(xmlNode.OuterXml);
                }
            }

            return sb.ToString();
        }

        private void GetInsertDVDxGenreCommand(List<string> sqlCommands, Profiler.DVD dvd, string genre)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxGenre VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_genreHash[genre]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeCustomCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_mediaTypeHash[dvd.MediaTypes.CustomMediaType]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeHDDVDCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_mediaTypeHash["HD-DVD"]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeBlurayCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_mediaTypeHash["Blu-ray"]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDxMediaTypeDVDCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVDxMediaType VALUES (");
            insertCommand.Append(this.IdCounter++);
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(_mediaTypeHash["DVD"]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertLockCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            if (dvd.Locks != null)
            {
                var insertCommand = new StringBuilder();

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

        private void GetInsertPurchaseCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            var insertCommand = new StringBuilder();

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
                if (!string.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.FirstName)
                    || !string.IsNullOrEmpty(dvd.PurchaseInfo.GiftFrom.LastName))
                {
                    var user = new Profiler.User(dvd.PurchaseInfo.GiftFrom);

                    insertCommand.Append(_userHash[user]);
                }
                else
                {
                    insertCommand.Append(NULL);
                }
            }

            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertFormatCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tFormat VALUES (");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.Format.AspectRatio));
            insertCommand.Append(", ");
            insertCommand.Append(_videoStandardHash[dvd.Format.VideoStandard]);
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

        private void GetInsertFeaturesCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            var insertCommand = new StringBuilder();

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

        private void GetInsertLoanInfoCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            var insertCommand = new StringBuilder();

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
                insertCommand.Append(_userHash[dvd.LoanInfo.User]);
            }

            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertReviewCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            var insertCommand = new StringBuilder();

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

        private void GetInsertDVDIdCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            var insertCommand = new StringBuilder();

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
            insertCommand.Append(_localityHash[dvd.ID_LocalityDesc]);
            insertCommand.Append(", ");
            insertCommand.Append(_dVDIdTypeHash[dvd.ID_Type]);
            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private void GetInsertDVDCommand(List<string> sqlCommands, Profiler.DVD dvd)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDVD VALUES (");
            insertCommand.Append(PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareTextForDb(dvd.UPC));
            insertCommand.Append(", ");
            insertCommand.Append(PrepareOptionalTextForDb(dvd.CollectionNumber));
            insertCommand.Append(", ");
            insertCommand.Append(_collectionTypeHash[dvd.CollectionType]);
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
                insertCommand.Append(_caseTypeHash[dvd.CaseType]);
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

        private void GetDividerData(ref string lastEpisode, ref string lastGroup, Profiler.Divider divider)
        {
            if (divider.Type == Profiler.DividerType.Episode)
            {
                lastEpisode = divider.Caption;

                lastGroup = null;
            }
            else if (divider.Type == Profiler.DividerType.Group)
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

        private void GetInsertDataCommands(List<string> sqlCommands, Profiler.DVD dvd, string countryOfOrigin)
        {
            if (string.IsNullOrEmpty(countryOfOrigin) == false)
            {
                StringBuilder insertCommand = new StringBuilder();

                insertCommand.Append("INSERT INTO tDVDxCountryOfOrigin VALUES (");
                insertCommand.Append(this.IdCounter++);
                insertCommand.Append(", ");
                insertCommand.Append(PrepareTextForDb(dvd.ID));
                insertCommand.Append(", ");
                insertCommand.Append(_countryOfOriginHash[countryOfOrigin]);
                insertCommand.Append(")");

                sqlCommands.Add(insertCommand.ToString());
            }
        }

        internal static void PrepareDateForDb(StringBuilder insertCommand, DateTime date, bool withTime)
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

        internal static string PrepareTextForDb(string text) => "'" + text.Replace("'", "''") + "'";

        internal static string PrepareOptionalTextForDb(string text) => string.IsNullOrEmpty(text) ? NULL : PrepareTextForDb(text);

        #endregion

        #region Insert...Data

        private void InsertBaseData()
        {
            var sqlCommands = new List<string>();

            this.GetInsertUserCommands(sqlCommands);

            this.InsertData(sqlCommands, "User");

            sqlCommands = new List<string>();

            this.GetInsertStudioAndMediaCompanyCommands(sqlCommands);

            this.InsertData(sqlCommands, "StudioAndMediaCompany");

            sqlCommands = new List<string>();

            this.GetInsertTagCommands(sqlCommands);

            this.InsertData(sqlCommands, "Tag");

            sqlCommands = new List<string>();

            this.GetInsertPluginDataCommands(sqlCommands);

            this.InsertData(sqlCommands, "PluginData");
        }

        private void InsertBaseData(Hashtable<string> hash
            , string tableName)
        {
            var sqlCommands = this.GetInsertBaseDataCommands(hash, tableName);

            this.InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertBaseData<T>(Hashtable<T> hash
            , string tableName)
            where T : struct
        {
            var sqlCommands = this.GetInsertBaseDataCommands(hash, tableName);

            this.InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertBaseData(CollectionTypeHashtable hash, string tableName)
        {
            var sqlCommands = this.GetInsertBaseDataCommands(hash, tableName);

            this.InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertBaseData(PersonHashtable hash, string tableName)
        {
            var sqlCommands = this.GetInsertBaseDataCommands(hash, tableName);

            this.InsertData(sqlCommands, tableName.Substring(1));
        }

        private void InsertData()
        {
            var profiles = this.GetInsertDataCommands();

            foreach (var profile in profiles)
            {
                foreach (var commandGroup in profile)
                {
                    if (commandGroup.Value.Count > 0)
                    {
                        this.InsertData(commandGroup.Value, commandGroup.Key);
                    }
                }
            }
        }

        private void InsertData(List<string> sqlCommands
            , string section)
        {
            ProgressMaxChanged?.Invoke(this, new EventArgs<int>(sqlCommands.Count));

            Feedback?.Invoke(this, new EventArgs<string>(section));

            int current = 0;

            foreach (string insertCommand in sqlCommands)
            {
                _command.CommandText = insertCommand;

                try
                {
                    _command.ExecuteNonQuery();
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
            _command.CommandText = "SELECT Version from tDBVersion";

            using (var reader = _command.ExecuteReader(System.Data.CommandBehavior.SingleRow))
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
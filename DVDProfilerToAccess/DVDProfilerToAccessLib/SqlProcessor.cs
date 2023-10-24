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
using DoenaSoft.ToolBox.Generics;
using Profiler = DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    public sealed class SqlProcessor
    {
        #region Fields

        private const string DVDProfilerSchemaVersion = "4.0.0.0";

        internal const string NULL = "NULL";

        private static readonly SqlProcessor _instance;

        internal int IdCounter { get; set; }

        private Dictionary<string> _audioChannels;

        private Dictionary<string> _audioContents;

        private Dictionary<string> _audioFormats;

        private Dictionary<string> _caseTypes;

        private CollectionTypeDictionary _collectionTypes;

        private Dictionary<Profiler.EventType> _eventTypes;

        private Dictionary<Profiler.DVDID_Type> _profileTypes;

        private Dictionary<Profiler.VideoStandard> _videoStandards;

        private Dictionary<string> _genres;

        private Dictionary<string> _subtitles;

        private Dictionary<string> _mediaTypes;

        private PersonDictionary _castAndCrewMembers;

        private Dictionary<string> _studiosAndMediaCompanies;

        private TagDictionary _tags;

        private UserDictionary _users;

        private Dictionary<Profiler.CategoryRestriction> _linkCategories;

        private Dictionary<string> _countriesOfOrigin;

        private Dictionary<string> _localities;

        private PluginDictionary _plugins;

        private Profiler.Collection _collection;

        private OleDbConnection _connection;

        private OleDbTransaction _transaction;

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
            var exceptionXml = this.Init(sourceFile, targetFile);

            if (exceptionXml == null)
            {
                exceptionXml = this.Execute(targetFile);
            }

            return exceptionXml;
        }

        private ExceptionXml Init(string sourceFile, string targetFile)
        {
            //Phase 2: Fill Dictionaries
            try
            {
                _profileTypes = this.FillStaticHash<Profiler.DVDID_Type>();

                _eventTypes = this.FillStaticHash<Profiler.EventType>();

                _videoStandards = this.FillStaticHash<Profiler.VideoStandard>();

                _linkCategories = this.FillStaticHash<Profiler.CategoryRestriction>();

                _collection = Serializer<Profiler.Collection>.Deserialize(sourceFile);

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
                Feedback?.Invoke(this, new EventArgs<string>($"Error: {exception.Message}"));

                var exceptionXml = new ExceptionXml(exception);

                return exceptionXml;
            }

            return null;
        }

        private ExceptionXml Execute(string targetFile)
        {
            _connection = null;

            _transaction = null;

            try
            {
                _connection = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + targetFile + ";Persist Security Info=True");

                _connection.Open();

                _transaction = _connection.BeginTransaction();

                this.CheckDBVersion();

                //Phase 3: Fill basic data inot Database
                this.InsertBasicData();

                //Phase 4: Fill profiles into Database
                this.InsertProfileData();

                //Phase 5: Save & Exit
                _transaction.Commit();

                _connection.Close();
            }
            catch (Exception exception)
            {
                try
                {
                    _transaction?.Rollback();
                }
                catch
                { }

                try

                {
                    _connection?.Close();
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

                Feedback?.Invoke(this, new EventArgs<string>($"Error: {exception.Message}"));

                var exceptionXml = new ExceptionXml(exception);

                return exceptionXml;
            }
            finally
            {
                try
                {
                    _transaction?.Dispose();
                }
                catch
                { }

                try
                {
                    _connection?.Dispose();
                }
                catch
                { }
            }

            return null;
        }

        private void InsertBasicData()
        {
            this.InsertBasicData(_localities, "tLocality");
            this.InsertBasicData(_profileTypes, "tDVDIdType");
            this.InsertBasicData(_audioChannels, "tAudioChannels");
            this.InsertBasicData(_audioContents, "tAudioContent");
            this.InsertBasicData(_audioFormats, "tAudioFormat");
            this.InsertBasicData(_caseTypes, "tCaseType");
            this.InsertBasicData(_collectionTypes, "tCollectionType");
            this.InsertBasicData(_eventTypes, "tEventType");
            this.InsertBasicData(_videoStandards, "tVideoStandard");
            this.InsertBasicData(_genres, "tGenre");
            this.InsertBasicData(_subtitles, "tSubtitle");
            this.InsertBasicData(_mediaTypes, "tMediaType");
            this.InsertBasicData(_castAndCrewMembers, "tCastAndCrew");
            this.InsertBasicData(_linkCategories, "tLinkCategory");
            this.InsertBasicData(_countriesOfOrigin, "tCountryOfOrigin");

            this.InsertUsers();

            this.InsertStudiosAndMediaCompanies();

            this.InsertTags();

            this.InsertPluginData();
        }

        #region Fill...Hash

        private Dictionary<T> FillStaticHash<T>() where T : struct
        {
            var fieldInfos = typeof(T).GetFields(BindingFlags.Public | BindingFlags.Static);

            if (fieldInfos?.Length > 0)
            {
                var data = new Dictionary<T>(fieldInfos.Length);

                foreach (var fieldInfo in fieldInfos)
                {
                    data.Add((T)fieldInfo.GetRawConstantValue());
                }

                return data;
            }
            else
            {
                return new Dictionary<T>(0);
            }
        }

        private void FillDynamicHash()
        {
            this.InitializeHashes();

            if (_collection.DVDList?.Length > 0)
            {

                foreach (var profile in _collection.DVDList)
                {
                    if (string.IsNullOrEmpty(profile.ID))
                    {
                        continue;
                    }

                    this.FillLocalityHash(profile);

                    this.FillCollectionTypeHash(profile);

                    this.FillCastHash(profile);

                    this.FillCrewHash(profile);

                    this.FillUserHashFromLoanInfo(profile);

                    this.FillUserHashFromEvents(profile);

                    this.FillStudioHash(profile);

                    this.FillMediaCompanyHash(profile);

                    this.FillTagHash(profile);

                    this.FillAudioHashes(profile);

                    this.FillCaseTypeHash(profile);

                    this.FillGenreHash(profile);

                    this.FillSubtitleHash(profile);

                    this.FillMediaTypeHash(profile);

                    this.FillCountryOfOriginHash(profile);

                    this.FillPluginHash(profile);
                }

                foreach (var profile in _collection.DVDList)
                {
                    //second iteration for data that is less complete
                    this.FillUserHashFromPurchaseInfo(profile);
                }
            }
        }

        private void InitializeHashes()
        {
            _localities = new Dictionary<string>(5);

            _collectionTypes = new CollectionTypeDictionary(5);

            _castAndCrewMembers = new PersonDictionary(_collection.DVDList.Length * 50);

            _studiosAndMediaCompanies = new Dictionary<string>(100);

            _audioChannels = new Dictionary<string>(20);

            _audioContents = new Dictionary<string>(20);

            _audioFormats = new Dictionary<string>(20);

            _caseTypes = new Dictionary<string>(20);

            _tags = new TagDictionary(50);

            _users = new UserDictionary(20);

            _genres = new Dictionary<string>(30);

            _subtitles = new Dictionary<string>(30);

            _mediaTypes = new Dictionary<string>(5);

            _countriesOfOrigin = new Dictionary<string>(20);

            _plugins = new PluginDictionary(5);
        }

        private void FillUserHashFromPurchaseInfo(Profiler.DVD profile)
        {
            if (profile.PurchaseInfo?.GiftFrom != null)
            {
                if (!string.IsNullOrEmpty(profile.PurchaseInfo.GiftFrom.FirstName)
                    || !string.IsNullOrEmpty(profile.PurchaseInfo.GiftFrom.LastName))
                {
                    var user = new Profiler.User(profile.PurchaseInfo.GiftFrom);

                    if (!_users.ContainsKey(user))
                    {
                        _users.Add(user);
                    }
                }
            }
        }

        private void FillPluginHash(Profiler.DVD profile)
        {
            if (profile.PluginCustomData?.Length > 0)
            {
                foreach (var pluginData in profile.PluginCustomData)
                {
                    if (pluginData != null && !_plugins.ContainsKey(pluginData))
                    {
                        _plugins.Add(pluginData);
                    }
                }
            }
        }

        private void FillCountryOfOriginHash(Profiler.DVD profile)
        {
            if (!string.IsNullOrEmpty(profile.CountryOfOrigin) && !_countriesOfOrigin.ContainsKey(profile.CountryOfOrigin))
            {
                _countriesOfOrigin.Add(profile.CountryOfOrigin);
            }

            if (!string.IsNullOrEmpty(profile.CountryOfOrigin2) && !_countriesOfOrigin.ContainsKey(profile.CountryOfOrigin2))
            {
                _countriesOfOrigin.Add(profile.CountryOfOrigin2);
            }

            if (!string.IsNullOrEmpty(profile.CountryOfOrigin3) && !_countriesOfOrigin.ContainsKey(profile.CountryOfOrigin3))
            {
                _countriesOfOrigin.Add(profile.CountryOfOrigin3);
            }
        }

        private void FillMediaTypeHash(Profiler.DVD profile)
        {
            if (profile.MediaTypes != null)
            {
                if (profile.MediaTypes.DVD && !_mediaTypes.ContainsKey("DVD"))
                {
                    _mediaTypes.Add("DVD");
                }

                if (profile.MediaTypes.BluRay && !_mediaTypes.ContainsKey("Blu-ray"))
                {
                    _mediaTypes.Add("Blu-ray");
                }

                if (profile.MediaTypes.HDDVD && !_mediaTypes.ContainsKey("HD-DVD"))
                {
                    _mediaTypes.Add("HD-DVD");
                }

                if (profile.MediaTypes.UltraHD && !_mediaTypes.ContainsKey("Ultra HD"))
                {
                    _mediaTypes.Add("Ultra HD");
                }

                if (!string.IsNullOrEmpty(profile.MediaTypes.CustomMediaType)
                    && !_mediaTypes.ContainsKey(profile.MediaTypes.CustomMediaType))
                {
                    _mediaTypes.Add(profile.MediaTypes.CustomMediaType);
                }
            }
        }

        private void FillSubtitleHash(Profiler.DVD profile)
        {
            if (profile.SubtitleList?.Length > 0)
            {
                foreach (var subtitle in profile.SubtitleList)
                {
                    if (!string.IsNullOrEmpty(subtitle) && !_subtitles.ContainsKey(subtitle))
                    {
                        _subtitles.Add(subtitle);
                    }
                }
            }
        }

        private void FillGenreHash(Profiler.DVD profile)
        {
            if (profile.GenreList?.Length > 0)
            {
                foreach (var genre in profile.GenreList)
                {
                    if (!string.IsNullOrEmpty(genre) && !_genres.ContainsKey(genre))
                    {
                        _genres.Add(genre);
                    }
                }
            }
        }

        private void FillCaseTypeHash(Profiler.DVD profile)
        {
            if (!string.IsNullOrEmpty(profile.CaseType))
            {
                if (!_caseTypes.ContainsKey(profile.CaseType))
                {
                    _caseTypes.Add(profile.CaseType);
                }
            }
        }

        private void FillAudioHashes(Profiler.DVD profile)
        {
            if (profile.AudioList?.Length > 0)
            {
                foreach (var audioTrack in profile.AudioList)
                {
                    if (!_audioContents.ContainsKey(audioTrack.Content))
                    {
                        _audioContents.Add(audioTrack.Content);
                    }

                    if (!_audioFormats.ContainsKey(audioTrack.Format))
                    {
                        _audioFormats.Add(audioTrack.Format);
                    }

                    if (!_audioChannels.ContainsKey(audioTrack.Channels))
                    {
                        _audioChannels.Add(audioTrack.Channels);
                    }

                }
            }
        }

        private void FillTagHash(Profiler.DVD profile)
        {
            if (profile.TagList?.Length > 0)
            {
                foreach (var tag in profile.TagList)
                {
                    if (_tags.ContainsKey(tag) == false)
                    {
                        _tags.Add(tag);
                    }
                }
            }
        }

        private void FillMediaCompanyHash(Profiler.DVD profile)
        {
            if (profile.MediaCompanyList?.Length > 0)
            {
                foreach (var distributor in profile.MediaCompanyList)
                {
                    if (!_studiosAndMediaCompanies.ContainsKey(distributor))
                    {
                        _studiosAndMediaCompanies.Add(distributor);
                    }
                }
            }
        }

        private void FillStudioHash(Profiler.DVD profile)
        {
            if (profile.StudioList?.Length > 0)
            {
                foreach (var studio in profile.StudioList)
                {
                    if (!_studiosAndMediaCompanies.ContainsKey(studio))
                    {
                        _studiosAndMediaCompanies.Add(studio);
                    }
                }
            }
        }

        private void FillUserHashFromEvents(Profiler.DVD profile)
        {
            if (profile.EventList?.Length > 0)
            {
                foreach (var myEvent in profile.EventList)
                {
                    if (!_users.ContainsKey(myEvent.User))
                    {
                        _users.Add(myEvent.User);
                    }
                }
            }
        }

        private void FillUserHashFromLoanInfo(Profiler.DVD profile)
        {
            if (profile.LoanInfo?.User != null)
            {
                if (!_users.ContainsKey(profile.LoanInfo.User))
                {
                    _users.Add(profile.LoanInfo.User);
                }
            }
        }

        private void FillCrewHash(Profiler.DVD profile)
        {
            if (profile.CrewList?.Length > 0)
            {
                foreach (var possibleCrew in profile.CrewList)
                {
                    this.FillDynamicHash<Profiler.CrewMember>(_castAndCrewMembers, possibleCrew);
                }
            }
        }

        private void FillCastHash(Profiler.DVD profile)
        {
            if (profile.CastList?.Length > 0)
            {
                foreach (var possibleCast in profile.CastList)
                {
                    this.FillDynamicHash<Profiler.CastMember>(_castAndCrewMembers, possibleCast);
                }
            }
        }

        private void FillCollectionTypeHash(Profiler.DVD profile)
        {
            if (!_collectionTypes.ContainsKey(profile.CollectionType))
            {
                _collectionTypes.Add(profile.CollectionType);
            }
        }

        private void FillLocalityHash(Profiler.DVD profile)
        {
            if (!_localities.ContainsKey(profile.ID_LocalityDesc))
            {
                _localities.Add(profile.ID_LocalityDesc);
            }
        }

        private void FillDynamicHash<T>(PersonDictionary personHash, object possiblePerson) where T : class, IPerson
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

        private void GetInsertPluginDataCommand(List<StringBuilder> commands, KeyValuePair<PluginDataKey, int> keyValue)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tPluginData VALUES (");
            commandText.Append(keyValue.Value.ToString());
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(keyValue.Key.ClassId.ToString()));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(keyValue.Key.Name));
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertTagCommand(List<StringBuilder> commands, KeyValuePair<TagKey, int> keyValue)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tTag VALUES (");
            commandText.Append(keyValue.Value.ToString());
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(keyValue.Key.Name));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(keyValue.Key.FullName));
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertStudioAndMediaCompanyCommand(List<StringBuilder> commands, KeyValuePair<string, int> keyValue)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tStudioAndMediaCompany VALUES (");
            commandText.Append(keyValue.Value.ToString());
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(keyValue.Key));
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertUserCommand(List<StringBuilder> commands, KeyValuePair<UserKey, int> keyValue)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tUser VALUES (");
            commandText.Append(keyValue.Value.ToString());
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(keyValue.Key.LastName));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(keyValue.Key.FirstName));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(keyValue.Key.EmailAddress));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(keyValue.Key.PhoneNumber));
            commandText.Append(")");

            commands.Add(commandText);
        }

        private List<StringBuilder> GetInsertBaseDataCommands(Dictionary<string> data, string tableName)
        {
            var commands = new List<StringBuilder>(data.Count);

            foreach (var keyValue in data)
            {
                var commandText = new StringBuilder();

                commandText.Append("INSERT INTO ");
                commandText.Append(tableName);
                commandText.Append(" VALUES (");
                commandText.Append(keyValue.Value.ToString());
                commandText.Append(", ");
                commandText.Append(PrepareTextForDb(keyValue.Key));
                commandText.Append(")");

                commands.Add(commandText);
            }

            return commands;
        }

        private List<StringBuilder> GetInsertBaseDataCommands<T>(Dictionary<T> data, string tableName) where T : struct
        {
            var commands = new List<StringBuilder>(data.Count);

            foreach (var keyValue in data)
            {
                var commandText = new StringBuilder();

                commandText.Append("INSERT INTO ");
                commandText.Append(tableName);
                commandText.Append(" VALUES (");
                commandText.Append(keyValue.Value.ToString());
                commandText.Append(", ");

                var name = Enum.GetName(typeof(T), keyValue.Key);

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

                commandText.Append(PrepareTextForDb(name));
                commandText.Append(")");

                commands.Add(commandText);
            }

            return commands;
        }

        private List<StringBuilder> GetInsertBaseDataCommands(PersonDictionary data, string tableName)
        {
            var commands = new List<StringBuilder>(data.Count);

            foreach (var keyValue in data)
            {
                var commandText = new StringBuilder();

                commandText.Append("INSERT INTO ");
                commandText.Append(tableName);
                commandText.Append(" VALUES (");
                commandText.Append(keyValue.Value.ToString());
                commandText.Append(", ");

                var keyData = keyValue.Key;

                commandText.Append(PrepareOptionalTextForDb(keyData.LastName));
                commandText.Append(", ");
                commandText.Append(PrepareOptionalTextForDb(keyData.MiddleName));
                commandText.Append(", ");
                commandText.Append(PrepareOptionalTextForDb(keyData.FirstName));
                commandText.Append(", ");

                if (keyData.BirthYear == 0)
                {
                    commandText.Append(NULL);
                }
                else
                {
                    commandText.Append(keyData.BirthYear);
                }

                commandText.Append(")");

                commands.Add(commandText);
            }

            return commands;
        }

        private List<StringBuilder> GetInsertBaseDataCommands(CollectionTypeDictionary data, string tableName)
        {
            var commands = new List<StringBuilder>(data.Count);

            foreach (var keyValue in data)
            {
                var commandText = new StringBuilder();

                commandText.Append("INSERT INTO ");
                commandText.Append(tableName);
                commandText.Append(" VALUES (");
                commandText.Append(keyValue.Value.ToString());
                commandText.Append(", ");
                commandText.Append(PrepareTextForDb(keyValue.Key.Value));
                commandText.Append(", ");
                commandText.Append(keyValue.Key.IsPartOfOwnedCollection);
                commandText.Append(")");

                commands.Add(commandText);
            }

            return commands;
        }

        private Dictionary<string, List<StringBuilder>> GetInsertPofileDataCommands(out HashSet<string> profileIds)
        {
            profileIds = new HashSet<string>();

            var commandGroups = new Dictionary<string, List<StringBuilder>>()
                {
                    { "DVD", new List<StringBuilder>() },
                    { "DVDId", new List<StringBuilder>() },
                    { "Review", new List<StringBuilder>() },
                    { "LoanInfo", new List<StringBuilder>() },
                    { "Features", new List<StringBuilder>() },
                    { "Format", new List<StringBuilder>() },
                    { "Purchase", new List<StringBuilder>() },
                    { "Lock", new List<StringBuilder>() },
                    { "DVDxMediaType", new List<StringBuilder>() },
                    { "DVDxGenre", new List<StringBuilder>() },
                    { "DVDxRegion", new List<StringBuilder>() },
                    { "DVDxStudio", new List<StringBuilder>() },
                    { "DVDxMediaCompany", new List<StringBuilder>() },
                    { "DVDxAudio", new List<StringBuilder>() },
                    { "DVDxSubtitle", new List<StringBuilder>() },
                    { "DVDxCast", new List<StringBuilder>() },
                    { "DVDxCrew", new List<StringBuilder>() },
                    { "DVDxDisc", new List<StringBuilder>() },
                    { "DVDxEvent", new List<StringBuilder>() },
                    { "DVDxTag", new List<StringBuilder>() },
                    { "DVDxMyLinks", new List<StringBuilder>() },
                    { "DVDxPlugin", new List<StringBuilder>() },
                    { "DVDxCountryOfOrigin", new List<StringBuilder>() },
                };

            foreach (var profile in _collection.DVDList)
            {
                if (string.IsNullOrEmpty(profile.ID))
                {
                    continue;
                }

                profileIds.Add(profile.ID);

                this.GetInsertDVDCommand(commandGroups["DVD"], profile);

                this.GetInsertDVDIdCommand(commandGroups["DVDId"], profile);

                this.GetInsertReviewCommand(commandGroups["Review"], profile);

                this.GetInsertLoanInfoCommand(commandGroups["LoanInfo"], profile);

                this.GetInsertFeaturesCommand(commandGroups["Features"], profile);

                this.GetInsertFormatCommand(commandGroups["Format"], profile);

                this.GetInsertPurchaseCommand(commandGroups["Purchase"], profile);

                this.GetInsertLockCommand(commandGroups["Lock"], profile);

                this.GetInsertDVDxMediaTypeCommands(commandGroups["DVDxMediaType"], profile);

                this.GetInsertDVDxGenreCommands(commandGroups["DVDxGenre"], profile);

                this.GetInsertDVDxRegionCommands(commandGroups["DVDxRegion"], profile);

                this.GetInsertDVDxStudioCommands(commandGroups["DVDxStudio"], profile);

                this.GetInsertDVDxMediaCompanyCommands(commandGroups["DVDxMediaCompany"], profile);

                this.GetInsertDVDxAudioCommands(commandGroups["DVDxAudio"], profile);

                this.GetInsertDVDxSubtitleCommands(commandGroups["DVDxSubtitle"], profile);

                this.GetInsertDVDxCastCommands(commandGroups["DVDxCast"], profile);

                this.GetInsertDVDxCrewCommands(commandGroups["DVDxCrew"], profile);

                this.GetInsertDVDxDiscCommands(commandGroups["DVDxDisc"], profile);

                this.GetInsertDVDxEventCommands(commandGroups["DVDxEvent"], profile);

                this.GetInsertDVDxTagCommands(commandGroups["DVDxTag"], profile);

                this.GetInsertDVDxMyLinksCommands(commandGroups["DVDxMyLinks"], profile);

                this.GetInsertDVDxPluginCommands(commandGroups["DVDxPlugin"], profile);

                this.GetInsertDvdXCountryOfOriginCommands(commandGroups["DVDxCountryOfOrigin"], profile);
            }

            return commandGroups;
        }

        private Dictionary<string, List<StringBuilder>> GetInsertBoxSetAssociationCommands(HashSet<string> profileIds)
        {
            var commandGroups = new Dictionary<string, List<StringBuilder>>()
            {
                { "BoxSetParent", new List<StringBuilder>() },
                { "BoxSetChildren", new List<StringBuilder>() },
            };

            foreach (var profile in _collection.DVDList)
            {
                this.GetUpdateParentDVDIdCommand(commandGroups["BoxSetParent"], profileIds, profile);

                this.GetInsertDVDxDVDCommands(commandGroups["BoxSetChildren"], profileIds, profile);
            }

            return commandGroups;
        }

        private void GetInsertDVDxMediaTypeCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.MediaTypes != null)
            {
                if (profile.MediaTypes.DVD)
                {
                    this.GetInsertDVDxMediaTypeCommand(commands, profile, "DVD");
                }

                if (profile.MediaTypes.BluRay)
                {
                    this.GetInsertDVDxMediaTypeCommand(commands, profile, "Blu-ray");
                }

                if (profile.MediaTypes.HDDVD)
                {
                    this.GetInsertDVDxMediaTypeCommand(commands, profile, "HD-DVD");
                }

                if (profile.MediaTypes.UltraHD)
                {
                    this.GetInsertDVDxMediaTypeCommand(commands, profile, "Ultra HD");
                }

                if (!string.IsNullOrEmpty(profile.MediaTypes.CustomMediaType))
                {
                    this.GetInsertDVDxMediaTypeCommand(commands, profile, profile.MediaTypes.CustomMediaType);
                }
            }
        }

        private void GetInsertDVDxDVDCommands(List<StringBuilder> commands, HashSet<string> profileIds, Profiler.DVD profile)
        {
            if (profile.BoxSet.ContentList?.Length > 0)
            {
                foreach (var childId in profile.BoxSet.ContentList)
                {
                    if (profileIds.Contains(childId))
                    {
                        this.GetInsertDVDxDVDCommand(commands, profile, childId);
                    }
                }
            }
        }

        private void GetInsertDVDxMyLinksCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.MyLinks?.UserLinkList?.Length > 0)
            {
                foreach (var userLink in profile.MyLinks.UserLinkList)
                {
                    this.GetInsertDVDxMyLinksCommand(commands, profile, userLink);
                }
            }
        }

        private void GetInsertDVDxTagCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.TagList?.Length > 0)
            {
                foreach (var tag in profile.TagList)
                {
                    this.GetInsertDVDxTagCommand(commands, profile, tag);
                }
            }
        }

        private void GetInsertDVDxEventCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.EventList?.Length > 0)
            {
                foreach (var myEvent in profile.EventList)
                {
                    this.GetInsertDVDxEventCommand(commands, profile, myEvent);
                }
            }
        }

        private void GetInsertDVDxDiscCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.DiscList?.Length > 0)
            {
                foreach (var disc in profile.DiscList)
                {
                    this.GetInsertDVDxDiscCommand(commands, profile, disc);
                }
            }
        }

        private void GetInsertDVDxCrewCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.CrewList?.Length > 0)
            {
                string lastEpisode = null;

                string lastGroup = null;

                string lastCreditType = null;

                foreach (var possibleCrew in profile.CrewList)
                {
                    if (possibleCrew is Profiler.CrewMember crew)
                    {
                        if (lastCreditType != crew.CreditType)
                        {
                            lastCreditType = crew.CreditType;

                            lastGroup = null;
                        }

                        this.GetInsertDVDxCrewCommand(commands, profile, crew, lastEpisode, lastGroup);
                    }
                    else
                    {
                        this.GetDividerData(ref lastEpisode, ref lastGroup, (Profiler.Divider)possibleCrew);
                    }
                }
            }
        }

        private void GetInsertDVDxCastCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.CastList?.Length > 0)
            {
                string lastEpisode = null;

                string lastGroup = null;

                foreach (var possibleCast in profile.CastList)
                {
                    if (possibleCast is Profiler.CastMember cast)
                    {
                        this.GetInsertDVDxCastCommand(commands, profile, cast, lastEpisode, lastGroup);
                    }
                    else
                    {
                        this.GetDividerData(ref lastEpisode, ref lastGroup, (Profiler.Divider)possibleCast);
                    }
                }
            }
        }

        private void GetInsertDVDxSubtitleCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.SubtitleList?.Length > 0)
            {
                foreach (var subtitle in profile.SubtitleList)
                {
                    if (string.IsNullOrEmpty(subtitle) == false)
                    {
                        this.GetInsertDVDxSubtitleCommand(commands, profile, subtitle);
                    }
                }
            }
        }

        private void GetInsertDVDxAudioCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.AudioList?.Length > 0)
            {
                foreach (var audio in profile.AudioList)
                {
                    this.GetInsertDVDxAudioCommand(commands, profile, audio);
                }
            }
        }

        private void GetInsertDVDxMediaCompanyCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.MediaCompanyList?.Length > 0)
            {
                foreach (var distributor in profile.MediaCompanyList)
                {
                    this.GetInsertDVDxMediaCompanyCommand(commands, profile, distributor);
                }
            }
        }

        private void GetInsertDVDxStudioCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.StudioList?.Length > 0)
            {
                foreach (var studio in profile.StudioList)
                {
                    this.GetInsertDVDxStudioCommand(commands, profile, studio);
                }
            }
        }

        private void GetInsertDVDxRegionCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.RegionList?.Length > 0)
            {
                foreach (var region in profile.RegionList)
                {
                    this.GetInsertDVDxRegionCommand(commands, profile, region);
                }
            }
        }

        private void GetInsertDVDxPluginCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.PluginCustomData?.Length > 0)
            {
                foreach (var pluginData in profile.PluginCustomData)
                {
                    if (pluginData != null)
                    {
                        this.GetInsertDVDxPluginCommand(commands, profile, pluginData);

                        PluginDataProcessor.AddInsertCommand(commands, profile, pluginData);
                    }
                }
            }
        }

        private void GetInsertDVDxGenreCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.GenreList?.Length > 0)
            {
                foreach (var genre in profile.GenreList)
                {
                    if (!string.IsNullOrEmpty(genre))
                    {
                        this.GetInsertDVDxGenreCommand(commands, profile, genre);
                    }
                }
            }
        }

        private void GetInsertDVDxDVDCommand(List<StringBuilder> commands, Profiler.DVD profile, string dvdId)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxDVD VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(dvdId));
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetUpdateParentDVDIdCommand(List<StringBuilder> commands, HashSet<string> profileIds, Profiler.DVD profile)
        {
            var parentId = profile.BoxSet.Parent;

            if (!string.IsNullOrEmpty(parentId) && profileIds.Contains(parentId))
            {
                var commandText = new StringBuilder();

                commandText.Append("UPDATE tDVD SET ParentDVDId = ");
                commandText.Append(PrepareTextForDb(parentId));
                commandText.Append(" WHERE Id = ");
                commandText.Append(PrepareTextForDb(profile.ID));

                commands.Add(commandText);
            }
        }

        private void GetInsertDVDxMyLinksCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.UserLink userLink)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxMyLinks VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(userLink.URL));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(userLink.Description));
            commandText.Append(", ");
            commandText.Append(_linkCategories[userLink.Category]);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxTagCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.Tag tag)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxTag VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(_tags[tag]);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxEventCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.Event myEvent)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxEvent VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(_eventTypes[myEvent.Type]);
            commandText.Append(", ");

            PrepareDateForDb(commandText, myEvent.Timestamp, true);

            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(myEvent.Note));
            commandText.Append(", ");
            commandText.Append(_users[myEvent.User]);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxDiscCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.Disc disc)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxDisc VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(disc.DescriptionSideA));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(disc.DescriptionSideB));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(disc.DiscIDSideA));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(disc.DiscIDSideB));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(disc.LabelSideA));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(disc.LabelSideB));
            commandText.Append(", ");
            commandText.Append(disc.DualLayeredSideA);
            commandText.Append(", ");
            commandText.Append(disc.DualLayeredSideB);
            commandText.Append(", ");
            commandText.Append(disc.DualSided);
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(disc.Location));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(disc.Slot));
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxCrewCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.CrewMember crew, string lastEpisode, string lastGroup)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxCrew VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(_castAndCrewMembers[crew]);
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(crew.CreditType));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(crew.CreditSubtype));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(crew.CreditedAs));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(lastEpisode));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(lastGroup));
            commandText.Append(", ");

            if (crew.CustomRoleSpecified)
            {
                commandText.Append(PrepareOptionalTextForDb(crew.CustomRole));
            }
            else
            {
                commandText.Append(NULL);
            }

            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxCastCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.CastMember cast, string lastEpisode, string lastGroup)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxCast VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(_castAndCrewMembers[cast]);
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(cast.Role));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(cast.CreditedAs));
            commandText.Append(", ");
            commandText.Append(cast.Voice);
            commandText.Append(", ");
            commandText.Append(cast.Uncredited);
            commandText.Append(", ");
            commandText.Append(cast.Puppeteer);
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(lastEpisode));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(lastGroup));
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxSubtitleCommand(List<StringBuilder> commands, Profiler.DVD profile, string subtitle)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxSubtitle VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(_subtitles[subtitle]);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxAudioCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.AudioTrack audio)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxAudio VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(_audioContents[audio.Content]);
            commandText.Append(", ");
            commandText.Append(_audioFormats[audio.Format]);
            commandText.Append(", ");
            commandText.Append(_audioChannels[audio.Channels]);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxMediaCompanyCommand(List<StringBuilder> commands, Profiler.DVD profile, string distributor)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxMediaCompany VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(_studiosAndMediaCompanies[distributor]);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxStudioCommand(List<StringBuilder> commands, Profiler.DVD profile, string studio)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxStudio VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(_studiosAndMediaCompanies[studio]);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxRegionCommand(List<StringBuilder> commands, Profiler.DVD profile, string region)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxRegion VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(region));
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxPluginCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.PluginData pluginData)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxPluginData VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(_plugins[pluginData]);
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(this.GetPluginData(pluginData.Any)));
            commandText.Append(")");

            commands.Add(commandText);
        }

        private string GetPluginData(XmlNode[] xmlNodes)
        {
            if ((xmlNodes == null) || (xmlNodes.Length == 0))
            {
                return null;
            }

            var pluginData = new StringBuilder();

            foreach (var xmlNode in xmlNodes)
            {
                if ((xmlNode != null) && (string.IsNullOrEmpty(xmlNode.OuterXml) == false))
                {
                    pluginData.AppendLine(xmlNode.OuterXml);
                }
            }

            return pluginData.ToString();
        }

        private void GetInsertDVDxGenreCommand(List<StringBuilder> commands, Profiler.DVD profile, string genre)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxGenre VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(_genres[genre]);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDxMediaTypeCommand(List<StringBuilder> commands, Profiler.DVD profile, string mediaType)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDxMediaType VALUES (");
            commandText.Append(this.IdCounter++);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(_mediaTypes[mediaType]);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertLockCommand(List<StringBuilder> commands, Profiler.DVD profile)
        {
            if (profile.Locks != null)
            {
                var commandText = new StringBuilder();

                commandText.Append("INSERT INTO tLock VALUES (");
                commandText.Append(PrepareTextForDb(profile.ID));
                commandText.Append(", ");
                commandText.Append(profile.Locks.Entire);
                commandText.Append(", ");
                commandText.Append(profile.Locks.Covers);
                commandText.Append(", ");
                commandText.Append(profile.Locks.Title);
                commandText.Append(", ");
                commandText.Append(profile.Locks.MediaType);
                commandText.Append(", ");
                commandText.Append(profile.Locks.Overview);
                commandText.Append(", ");
                commandText.Append(profile.Locks.Regions);
                commandText.Append(", ");
                commandText.Append(profile.Locks.Genres);
                commandText.Append(", ");
                commandText.Append(profile.Locks.SRP);
                commandText.Append(", ");
                commandText.Append(profile.Locks.Studios);
                commandText.Append(", ");
                commandText.Append(profile.Locks.DiscInformation);
                commandText.Append(", ");
                commandText.Append(profile.Locks.Cast);
                commandText.Append(", ");
                commandText.Append(profile.Locks.Crew);
                commandText.Append(", ");
                commandText.Append(profile.Locks.Features);
                commandText.Append(", ");
                commandText.Append(profile.Locks.AudioTracks);
                commandText.Append(", ");
                commandText.Append(profile.Locks.Subtitles);
                commandText.Append(", ");
                commandText.Append(profile.Locks.EasterEggs);
                commandText.Append(", ");
                commandText.Append(profile.Locks.RunningTime);
                commandText.Append(", ");
                commandText.Append(profile.Locks.ReleaseDate);
                commandText.Append(", ");
                commandText.Append(profile.Locks.ProductionYear);
                commandText.Append(", ");
                commandText.Append(profile.Locks.CaseType);
                commandText.Append(", ");
                commandText.Append(profile.Locks.VideoFormats);
                commandText.Append(", ");
                commandText.Append(profile.Locks.Rating);
                commandText.Append(")");

                commands.Add(commandText);
            }
        }

        private void GetInsertPurchaseCommand(List<StringBuilder> commands, Profiler.DVD profile)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tPurchase VALUES (");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");

            if (profile.PurchaseInfo.Price.Value == 0.0f)
            {
                commandText.Append(NULL);
                commandText.Append(", ");
                commandText.Append(NULL);
            }
            else
            {
                commandText.Append(PrepareTextForDb(profile.PurchaseInfo.Price.DenominationType));
                commandText.Append(", ");
                commandText.Append(profile.PurchaseInfo.Price.Value.ToString(FormatInfo));
            }

            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.PurchaseInfo.Place));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.PurchaseInfo.Type));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.PurchaseInfo.Website));
            commandText.Append(", ");

            if (profile.PurchaseInfo.DateSpecified == false)
            {
                commandText.Append(NULL);
            }
            else
            {
                PrepareDateForDb(commandText, profile.PurchaseInfo.Date, false);
            }

            commandText.Append(", ");
            commandText.Append(profile.PurchaseInfo.ReceivedAsGift);
            commandText.Append(", ");

            if (profile.PurchaseInfo.GiftFrom == null)
            {
                commandText.Append(NULL);
            }
            else
            {
                if (!string.IsNullOrEmpty(profile.PurchaseInfo.GiftFrom.FirstName)
                    || !string.IsNullOrEmpty(profile.PurchaseInfo.GiftFrom.LastName))
                {
                    var user = new Profiler.User(profile.PurchaseInfo.GiftFrom);

                    commandText.Append(_users[user]);
                }
                else
                {
                    commandText.Append(NULL);
                }
            }

            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertFormatCommand(List<StringBuilder> commands, Profiler.DVD profile)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tFormat VALUES (");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.Format.AspectRatio));
            commandText.Append(", ");
            commandText.Append(_videoStandards[profile.Format.VideoStandard]);
            commandText.Append(", ");
            commandText.Append(profile.Format.LetterBox);
            commandText.Append(", ");
            commandText.Append(profile.Format.PanAndScan);
            commandText.Append(", ");
            commandText.Append(profile.Format.FullFrame);
            commandText.Append(", ");
            commandText.Append(profile.Format.Enhanced16X9);
            commandText.Append(", ");
            commandText.Append(profile.Format.DualSided);
            commandText.Append(", ");
            commandText.Append(profile.Format.DualLayered);
            commandText.Append(", ");
            commandText.Append(profile.Format.Color.Color);
            commandText.Append(", ");
            commandText.Append(profile.Format.Color.BlackAndWhite);
            commandText.Append(", ");
            commandText.Append(profile.Format.Color.Colorized);
            commandText.Append(", ");
            commandText.Append(profile.Format.Color.Mixed);
            commandText.Append(", ");
            commandText.Append(profile.Format.Dimensions.Dim2D);
            commandText.Append(", ");
            commandText.Append(profile.Format.Dimensions.Dim3DAnaglyph);
            commandText.Append(", ");
            commandText.Append(profile.Format.Dimensions.Dim3DBluRay);
            commandText.Append(", ");
            commandText.Append(profile.Format.DynamicRange?.DRHDR10.ToString() ?? NULL);
            commandText.Append(", ");
            commandText.Append(profile.Format.DynamicRange?.DRDolbyVision.ToString() ?? NULL);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertFeaturesCommand(List<StringBuilder> commands, Profiler.DVD profile)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tFeatures VALUES (");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(profile.Features.SceneAccess);
            commandText.Append(", ");
            commandText.Append(profile.Features.Commentary);
            commandText.Append(", ");
            commandText.Append(profile.Features.Trailer);
            commandText.Append(", ");
            commandText.Append(profile.Features.PhotoGallery);
            commandText.Append(", ");
            commandText.Append(profile.Features.DeletedScenes);
            commandText.Append(", ");
            commandText.Append(profile.Features.MakingOf);
            commandText.Append(", ");
            commandText.Append(profile.Features.ProductionNotes);
            commandText.Append(", ");
            commandText.Append(profile.Features.Game);
            commandText.Append(", ");
            commandText.Append(profile.Features.DVDROMContent);
            commandText.Append(", ");
            commandText.Append(profile.Features.MultiAngle);
            commandText.Append(", ");
            commandText.Append(profile.Features.MusicVideos);
            commandText.Append(", ");
            commandText.Append(profile.Features.Interviews);
            commandText.Append(", ");
            commandText.Append(profile.Features.StoryboardComparisons);
            commandText.Append(", ");
            commandText.Append(profile.Features.Outtakes);
            commandText.Append(", ");
            commandText.Append(profile.Features.ClosedCaptioned);
            commandText.Append(", ");
            commandText.Append(profile.Features.THXCertified);
            commandText.Append(", ");
            commandText.Append(profile.Features.PIP);
            commandText.Append(", ");
            commandText.Append(profile.Features.BDLive);
            commandText.Append(", ");
            commandText.Append(profile.Features.BonusTrailers);
            commandText.Append(", ");
            commandText.Append(profile.Features.DigitalCopy);
            commandText.Append(", ");
            commandText.Append(profile.Features.DBOX);
            commandText.Append(", ");
            commandText.Append(profile.Features.CineChat);
            commandText.Append(", ");
            commandText.Append(profile.Features.PlayAll);
            commandText.Append(", ");
            commandText.Append(profile.Features.MovieIQ);
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.Features.OtherFeatures));
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertLoanInfoCommand(List<StringBuilder> commands, Profiler.DVD profile)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tLoanInfo VALUES (");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(profile.LoanInfo.Loaned);
            commandText.Append(", ");

            if (profile.LoanInfo.DueSpecified == false)
            {
                commandText.Append(NULL);
            }
            else
            {
                PrepareDateForDb(commandText, profile.LoanInfo.Due, false);
            }

            commandText.Append(", ");

            if (profile.LoanInfo.User == null)
            {
                commandText.Append(NULL);
            }
            else
            {
                commandText.Append(_users[profile.LoanInfo.User]);
            }

            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertReviewCommand(List<StringBuilder> commands, Profiler.DVD profile)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tReview VALUES (");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(profile.Review.Film);
            commandText.Append(", ");
            commandText.Append(profile.Review.Video);
            commandText.Append(", ");
            commandText.Append(profile.Review.Audio);
            commandText.Append(", ");
            commandText.Append(profile.Review.Extras);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDIdCommand(List<StringBuilder> commands, Profiler.DVD profile)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVDId VALUES (");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.ID_Base));
            commandText.Append(", ");

            if (profile.ID_VariantNum > 0)
            {
                commandText.Append(profile.ID_VariantNum);
            }
            else
            {
                commandText.Append(NULL);
            }

            commandText.Append(", ");

            if (profile.ID_LocalityID > 0)
            {
                commandText.Append(profile.ID_LocalityID);
            }
            else
            {
                commandText.Append(NULL);
            }

            commandText.Append(", ");
            commandText.Append(_localities[profile.ID_LocalityDesc]);
            commandText.Append(", ");
            commandText.Append(_profileTypes[profile.ID_Type]);
            commandText.Append(")");

            commands.Add(commandText);
        }

        private void GetInsertDVDCommand(List<StringBuilder> commands, Profiler.DVD profile)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDVD VALUES (");
            commandText.Append(PrepareTextForDb(profile.ID));
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.UPC));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.CollectionNumber));
            commandText.Append(", ");
            commandText.Append(_collectionTypes[profile.CollectionType]);
            commandText.Append(", ");
            commandText.Append(PrepareTextForDb(profile.Title));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.Edition));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.OriginalTitle));
            commandText.Append(", ");

            if (profile.ProductionYear == 0)
            {
                commandText.Append(NULL);
            }
            else
            {
                commandText.Append(profile.ProductionYear);
            }

            commandText.Append(", ");

            if (profile.ReleasedSpecified == false)
            {
                commandText.Append(NULL);
            }
            else
            {
                PrepareDateForDb(commandText, profile.Released, false);
            }

            commandText.Append(", ");

            if (profile.RunningTime == 0)
            {
                commandText.Append(NULL);
            }
            else
            {
                commandText.Append(profile.RunningTime);
            }

            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.Rating));
            commandText.Append(", ");

            if (string.IsNullOrEmpty(profile.CaseType) == false)
            {
                commandText.Append(_caseTypes[profile.CaseType]);
            }
            else
            {
                commandText.Append("NULL");
            }

            commandText.Append(", ");

            if (profile.CaseSlipCoverSpecified == false)
            {
                commandText.Append(NULL);
            }
            else
            {
                commandText.Append(profile.CaseSlipCover);
            }

            commandText.Append(", ");
            commandText.Append(NULL); //BoxSetParent
            commandText.Append(", ");

            if (profile.SRP.Value == 0.0f)
            {
                commandText.Append(NULL);
                commandText.Append(", ");
                commandText.Append(NULL);
            }
            else
            {
                commandText.Append(PrepareTextForDb(profile.SRP.DenominationType));
                commandText.Append(", ");
                commandText.Append(profile.SRP.Value.ToString(FormatInfo));
            }

            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.Overview));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.EasterEggs));
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.SortTitle));
            commandText.Append(", ");

            PrepareDateForDb(commandText, profile.LastEdited, true);

            commandText.Append(", ");
            commandText.Append(profile.WishPriority);
            commandText.Append(", ");
            commandText.Append(PrepareOptionalTextForDb(profile.Notes));
            commandText.Append(")");

            commands.Add(commandText);
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

        private void GetInsertDvdXCountryOfOriginCommands(List<StringBuilder> commands, Profiler.DVD profile)
        {
            this.GetInsertDvdXCountryOfOriginCommand(commands, profile, profile.CountryOfOrigin);
            this.GetInsertDvdXCountryOfOriginCommand(commands, profile, profile.CountryOfOrigin2);
            this.GetInsertDvdXCountryOfOriginCommand(commands, profile, profile.CountryOfOrigin3);
        }

        private void GetInsertDvdXCountryOfOriginCommand(List<StringBuilder> commands, Profiler.DVD profile, string countryOfOrigin)
        {
            if (!string.IsNullOrEmpty(countryOfOrigin))
            {
                var commandText = new StringBuilder();

                commandText.Append("INSERT INTO tDVDxCountryOfOrigin VALUES (");
                commandText.Append(this.IdCounter++);
                commandText.Append(", ");
                commandText.Append(PrepareTextForDb(profile.ID));
                commandText.Append(", ");
                commandText.Append(_countriesOfOrigin[countryOfOrigin]);
                commandText.Append(")");

                commands.Add(commandText);
            }
        }

        #endregion

        #region Prepare...ForDb

        internal static void PrepareDateForDb(StringBuilder commandText, DateTime date, bool withTime)
        {
            commandText.Append("#");
            commandText.Append(date.Month);
            commandText.Append("/");
            commandText.Append(date.Day);
            commandText.Append("/");
            commandText.Append(date.Year);

            if (withTime)
            {
                commandText.Append(" ");
                commandText.Append(date.Hour.ToString("00"));
                commandText.Append(":");
                commandText.Append(date.Minute.ToString("00"));
                commandText.Append(":");
                commandText.Append(date.Second.ToString("00"));
            }

            commandText.Append("#");
        }

        internal static string PrepareTextForDb(string text) => $"'{text.Replace("'", "''")}'";

        internal static string PrepareOptionalTextForDb(string text) => string.IsNullOrEmpty(text) ? NULL : PrepareTextForDb(text);

        #endregion

        #region Insert...Data

        private void InsertBasicData(Dictionary<string> data, string tableName)
        {
            var commands = this.GetInsertBaseDataCommands(data, tableName);

            this.InsertData(commands, tableName.Substring(1));
        }

        private void InsertBasicData<T>(Dictionary<T> data, string tableName) where T : struct
        {
            var commands = this.GetInsertBaseDataCommands(data, tableName);

            this.InsertData(commands, tableName.Substring(1));
        }

        private void InsertBasicData(CollectionTypeDictionary data, string tableName)
        {
            var commands = this.GetInsertBaseDataCommands(data, tableName);

            this.InsertData(commands, tableName.Substring(1));
        }

        private void InsertBasicData(PersonDictionary data, string tableName)
        {
            var commands = this.GetInsertBaseDataCommands(data, tableName);

            this.InsertData(commands, tableName.Substring(1));
        }

        private void InsertPluginData()
        {
            var commands = new List<StringBuilder>();

            foreach (var keyValue in _plugins)
            {
                this.GetInsertPluginDataCommand(commands, keyValue);
            }

            this.InsertData(commands, "PluginData");
        }

        private void InsertTags()
        {
            var commands = new List<StringBuilder>();

            foreach (var keyValue in _tags)
            {
                this.GetInsertTagCommand(commands, keyValue);
            }

            this.InsertData(commands, "Tag");
        }

        private void InsertStudiosAndMediaCompanies()
        {
            var commands = new List<StringBuilder>();

            foreach (var keyValue in _studiosAndMediaCompanies)
            {
                this.GetInsertStudioAndMediaCompanyCommand(commands, keyValue);
            }

            this.InsertData(commands, "StudioAndMediaCompany");
        }

        private void InsertUsers()
        {
            var commands = new List<StringBuilder>();

            foreach (var keyValue in _users)
            {
                this.GetInsertUserCommand(commands, keyValue);
            }

            this.InsertData(commands, "User");
        }

        private void InsertProfileData()
        {
            var commandGroups = this.GetInsertPofileDataCommands(out var profileIds);

            foreach (var commandGroup in commandGroups)
            {
                this.InsertData(commandGroup.Value, commandGroup.Key);
            }

            commandGroups = this.GetInsertBoxSetAssociationCommands(profileIds);

            foreach (var commandGroup in commandGroups)
            {
                this.InsertData(commandGroup.Value, commandGroup.Key);
            }
        }

        private void InsertData(List<StringBuilder> commands, string section)
        {
            if (commands.Count > 0)
            {
                ProgressMaxChanged?.Invoke(this, new EventArgs<int>(commands.Count));

                Feedback?.Invoke(this, new EventArgs<string>(section));

                var current = 0;

                foreach (var command in commands)
                {
                    this.ExecuteCommand(command.ToString());

                    ProgressValueChanged?.Invoke(this, new EventArgs<int>(current));

                    current++;
                }

                ProgressMaxChanged?.Invoke(this, new EventArgs<int>(0));
            }
        }

        private void ExecuteCommand(string commandText)
        {
            using (var command = _connection.CreateCommand())
            {
                command.Transaction = _transaction;

                command.CommandText = commandText;

                try
                {
                    command.ExecuteNonQuery();
                }
                catch (OleDbException ex)
                {
                    throw new ApplicationException($"Error at query:{Environment.NewLine}{commandText}", ex);
                }
            }
        }

        #endregion

        private void CheckDBVersion()
        {
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = "SELECT Version from tDBVersion";

                command.Transaction = _transaction;

                using (var reader = command.ExecuteReader(System.Data.CommandBehavior.SingleRow))
                {
                    reader.Read();

                    var version = reader.GetString(0);

                    if (version != DVDProfilerSchemaVersion)
                    {
                        throw new InvalidOperationException("Error: Database version incorrect. Abort.");
                    }
                }
            }
        }
    }
}
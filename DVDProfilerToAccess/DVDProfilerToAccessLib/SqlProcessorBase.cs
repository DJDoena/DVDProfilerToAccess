namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Reflection;
    using DVDProfilerHelper;
    using DVDProfilerXML;
    using Profiler = DVDProfilerXML.Version400;

    public abstract class SqlProcessorBase
    {
        internal int IdCounter { get; set; }

        protected Dictionary<string> _audioChannelsHash;

        protected Dictionary<string> _audioContentHash;

        protected Dictionary<string> _audioFormatHash;

        protected Dictionary<string> _caseTypeHash;

        protected CollectionTypeHashtable _collectionTypeHash;

        protected Dictionary<Profiler.EventType> _eventTypeHash;

        protected Dictionary<Profiler.DVDID_Type> _dVDIdTypeHash;

        protected Dictionary<Profiler.VideoStandard> _videoStandardHash;

        protected Dictionary<string> _genreHash;

        protected Dictionary<string> _subtitleHash;

        protected Dictionary<string> _mediaTypeHash;

        protected PersonHashtable _castAndCrewHash;

        protected Dictionary<string> _studioAndMediaCompanyHash;

        protected TagHashtable _tagHash;

        protected UserHashtable _userHash;

        protected Dictionary<Profiler.CategoryRestriction> _linkCategoryHash;

        protected Dictionary<string> _countryOfOriginHash;

        protected Dictionary<string> _localityHash;

        protected PluginHashtable _pluginHash;

        protected Profiler.Collection _collection;

        internal static NumberFormatInfo FormatInfo { get; }

        static SqlProcessorBase()
        {
            FormatInfo = CultureInfo.GetCultureInfo("en-US").NumberFormat;
        }

        protected SqlProcessorBase()
        {
            this.IdCounter = 1;
        }

        public event EventHandler<EventArgs<int>> ProgressMaxChanged;

        public event EventHandler<EventArgs<int>> ProgressValueChanged;

        public event EventHandler<EventArgs<string>> Feedback;

        protected ExceptionXml Init(string sourceFile, string targetFile)
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
                this.RaiseFeedback($"Error: {exception.Message}");

                var exceptionXml = new ExceptionXml(exception);

                return exceptionXml;
            }

            return null;
        }

        protected void RaiseFeedback(string message) => Feedback?.Invoke(this, new EventArgs<string>(message));

        protected void RaiseProgressMax(int max) => ProgressMaxChanged?.Invoke(this, new EventArgs<int>(max));

        protected void RaiseProgressValue(int current) => ProgressValueChanged?.Invoke(this, new EventArgs<int>(current));

        #region Fill...Hash

        private Dictionary<T> FillStaticHash<T>() where T : struct
        {
            var fieldInfos = typeof(T).GetFields(BindingFlags.Public | BindingFlags.Static);

            if (fieldInfos?.Length > 0)
            {
                Dictionary<T> hash = new Dictionary<T>(fieldInfos.Length);

                foreach (FieldInfo fieldInfo in fieldInfos)
                {
                    hash.Add((T)fieldInfo.GetRawConstantValue());
                }

                return hash;
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
            _localityHash = new Dictionary<string>(5);

            _collectionTypeHash = new CollectionTypeHashtable(5);

            _castAndCrewHash = new PersonHashtable(_collection.DVDList.Length * 50);

            _studioAndMediaCompanyHash = new Dictionary<string>(100);

            _audioChannelsHash = new Dictionary<string>(20);

            _audioContentHash = new Dictionary<string>(20);

            _audioFormatHash = new Dictionary<string>(20);

            _caseTypeHash = new Dictionary<string>(20);

            _tagHash = new TagHashtable(50);

            _userHash = new UserHashtable(20);

            _genreHash = new Dictionary<string>(30);

            _subtitleHash = new Dictionary<string>(30);

            _mediaTypeHash = new Dictionary<string>(5);

            _countryOfOriginHash = new Dictionary<string>(20);

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
    }
}

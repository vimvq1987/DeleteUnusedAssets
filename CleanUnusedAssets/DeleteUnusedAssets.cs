using EPiServer;
using EPiServer.Core;
using EPiServer.Framework.Blobs;
using EPiServer.Logging;
using EPiServer.PlugIn;
using EPiServer.Scheduler;
using EPiServer.Web;
using System.Globalization;

namespace vimvq1987
{
    /// <summary>
    /// Download a catalog.
    /// </summary>
    [ScheduledPlugIn(DisplayName = "Delete unused assets", Description = "Delete unused assets", SortIndex = 10001)]
    public class DeleteUnusedAssets : ScheduledJobBase
    {
        private static readonly ILogger _logger = LogManager.GetLogger(typeof(DeleteUnusedAssets));
        private readonly IContentRepository _contentRepository;
        private readonly IBlobFactory _blobFactory;
        private readonly IPermanentLinkMapper _permanentLinkMapper;
        private bool _isStopped;

        /// <summary>
        /// Initializes a new instance of the <see cref="CatalogIndexingJob"/> class.
        /// </summary>
        public DeleteUnusedAssets(IContentRepository contentRepository, IBlobFactory blobFactory, IPermanentLinkMapper permanentLinkMapper)
        {
            IsStoppable = true;
            _contentRepository = contentRepository;
            _blobFactory = blobFactory;
            _permanentLinkMapper = permanentLinkMapper;
        }

        public override string Execute()
        {
            _isStopped = false;
            var toDelete = new HashSet<ContentReference>(ContentReferenceComparer.IgnoreVersion);
            var totalAsset = 0;
            var deletedAsset = 0;
            foreach (var asset in GetAssetRecursive<MediaData>(SiteDefinition.Current.GlobalAssetsRoot, CultureInfo.InvariantCulture))
            {
                totalAsset++;
                if (!_contentRepository.GetReferencesToContent(asset.ContentLink, false).Any())
                {
                    //TODO: make this configurable
                    if (asset.Created < DateTime.UtcNow.AddDays(-30))
                    {
                        toDelete.Add(asset.ContentLink.ToReferenceWithoutVersion());
                    }
                }

                if (toDelete.Count % 50 == 0)
                {
                    var maps = _permanentLinkMapper.Find(toDelete);
                    foreach (var map in maps)
                    {
                        deletedAsset++;
                        _contentRepository.Delete(map.ContentReference, true, EPiServer.Security.AccessLevel.NoAccess);
                        var container = Blob.GetContainerIdentifier(map.Guid);
                        //Probably redundency, can just delete directly
                        var blob = _blobFactory.GetBlob(container);
                        if (blob != null)
                        {
                            _blobFactory.Delete(container);
                        }
                        OnStatusChanged($"Deleting asset with id {map.ContentReference}");
                    }
                    toDelete.Clear();
                }
            }

            foreach (var contentLink in toDelete)
            {
                deletedAsset++;
                _contentRepository.Delete(contentLink, true, EPiServer.Security.AccessLevel.NoAccess);
                OnStatusChanged($"Deleting asset with id {contentLink}");
            }

            return $"Removed {deletedAsset} unused blobs in {totalAsset} total";
        }

        public virtual IEnumerable<T> GetAssetRecursive<T>(ContentReference parentLink, CultureInfo defaultCulture) where T : MediaData
        {
            foreach (var folder in LoadChildrenBatched<ContentFolder>(parentLink, defaultCulture))
            {
                foreach (var entry in GetAssetRecursive<T>(folder.ContentLink, defaultCulture))
                {
                    yield return entry;
                }
            }

            foreach (var entry in LoadChildrenBatched<T>(parentLink, defaultCulture))
            {
                yield return entry;
            }
        }

        private IEnumerable<T> LoadChildrenBatched<T>(ContentReference parentLink, CultureInfo defaultCulture) where T : IContent
        {
            var start = 0;

            while (!_isStopped)
            {
                var batch = _contentRepository.GetChildren<T>(parentLink, defaultCulture, start, 50);
                if (!batch.Any())
                {
                    yield break;
                }
                foreach (var content in batch)
                {
                    // Don't include linked products to avoid including them multiple times when traversing the catalog
                    if (!parentLink.CompareToIgnoreWorkID(content.ParentLink))
                    {
                        continue;
                    }

                    yield return content;
                }
                start += 50;
            }
        }

        public override void Stop()
        {
            _isStopped = true;
        }
    }
}
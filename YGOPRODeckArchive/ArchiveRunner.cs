using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Runtime.CompilerServices;

namespace YGOPRODeckArchive;

internal sealed class ArchiveRunner
{
    private static readonly string[] Languages = ["en", "fr", "de", "it", "pt"];
    private const int SetProgressLogInterval = 25;
    private const int MaxConcurrentCardProcessors = 6;
    private const int MaxConcurrentSetProcessors = 6;
    private const int MaxConcurrentTranslationDatasets = 4;
    private const int MaxConcurrentDownloads = 8;

    private readonly SemaphoreSlim _downloadLimiter = new(MaxConcurrentDownloads, MaxConcurrentDownloads);
    private readonly SemaphoreSlim _stateGate = new(1, 1);

    private ArchiveLayout? _layout;
    private ArchiveLogger? _logger;
    private ArchiveConsoleProgress? _consoleProgress;
    private YgoProDeckApiClient? _apiClient;
    private ArchiveStateDocument _state = new();
    private readonly Dictionary<int, CardIndexEntryDocument> _cardIndex = [];
    private readonly Dictionary<int, HashSet<string>> _cardLanguages = [];
    private readonly Dictionary<string, SetIndexEntryDocument> _setIndex = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, HashSet<int>> _setMembership = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<NormalizedSetDefinition>> _setsByName = new(StringComparer.OrdinalIgnoreCase);

    public async Task<int> RunAsync(CliOptions options, CancellationToken cancellationToken)
    {
        return await ArchiveConsoleProgress.RunAsync(progress => RunCoreAsync(options, progress, cancellationToken));
    }

    private async Task<int> RunCoreAsync(
        CliOptions options,
        ArchiveConsoleProgress? consoleProgress,
        CancellationToken cancellationToken)
    {
        _layout = ArchivePaths.Create(options.RootPath);
        _consoleProgress = consoleProgress;
        await using ArchiveLogger logger = await ArchiveLogger.CreateAsync(_layout, consoleProgress, cancellationToken);
        await using YgoProDeckApiClient apiClient = new();

        _logger = logger;
        _apiClient = apiClient;

        try
        {
            await LoadExistingStateAsync(cancellationToken);
            await LoadExistingIndexesAsync(cancellationToken);

            await _stateGate.WaitAsync(cancellationToken);

            try
            {
                _state.SchemaVersion = 1;
                _state.AppVersion = ArchivePaths.GetApplicationVersion();
                _state.ArchiveRoot = _layout.RootDirectory;
                _state.LastRunStartedUtc = DateTimeOffset.UtcNow;
                _state.LastRunCompletedUtc = null;
                _state.LastRunStatus = "running";

                await PersistStateUnsafeAsync(cancellationToken);
            }
            finally
            {
                _stateGate.Release();
            }

            await _logger.InfoAsync($"Archive root: {_layout.RootDirectory}", cancellationToken);
            await _logger.InfoAsync($"Page size: {options.PageSize}", cancellationToken);
            await _logger.InfoAsync($"Keep raw pages: {options.KeepRawPages}", cancellationToken);
            await _logger.InfoAsync($"Force refresh: {options.ForceRefresh}", cancellationToken);
            _consoleProgress?.SetActivity("Preparing archive");
            await LogRunStartSummaryAsync(cancellationToken);

            (List<NormalizedSetDefinition> setDefinitions, DatasetStateDocument setsDataset) = await LoadSetDefinitionsAsync(options, cancellationToken);
            await ArchiveCardDatasetAsync(
                "en",
                options,
                (card, innerOptions, innerCancellationToken) => ProcessEnglishCardAsync(card, innerOptions, innerCancellationToken),
                cancellationToken);

            Task setTask = WriteSetFilesAsync(setsDataset, setDefinitions, options, cancellationToken);
            Task translationTask = ArchiveTranslationsAsync(options, cancellationToken);
            await Task.WhenAll(setTask, translationTask);

            await PersistIndexesAsync(cancellationToken);

            await _stateGate.WaitAsync(cancellationToken);

            try
            {
                ClearFailureUnsafe("run");
                _state.LastRunCompletedUtc = DateTimeOffset.UtcNow;
                _state.LastRunStatus = "completed";
                await PersistStateUnsafeAsync(cancellationToken);
            }
            finally
            {
                _stateGate.Release();
            }

            await LogRunCompletionSummaryAsync("completed", cancellationToken);
            await _logger.InfoAsync("Archive run completed successfully.", cancellationToken);
            return 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await _stateGate.WaitAsync(CancellationToken.None);

            try
            {
                _state.LastRunCompletedUtc = DateTimeOffset.UtcNow;
                _state.LastRunStatus = "cancelled";
                await PersistIndexesUnsafeAsync(CancellationToken.None);
                await PersistStateUnsafeAsync(CancellationToken.None);
            }
            finally
            {
                _stateGate.Release();
            }

            await LogRunCompletionSummaryAsync("cancelled", CancellationToken.None);
            await _logger.WarnAsync("Archive run cancelled.", CancellationToken.None);
            return 2;
        }
        catch (Exception exception)
        {
            await _stateGate.WaitAsync(CancellationToken.None);

            try
            {
                RecordFailureUnsafe("run", "run", exception.Message);
                _state.LastRunCompletedUtc = DateTimeOffset.UtcNow;
                _state.LastRunStatus = "failed";
                await PersistIndexesUnsafeAsync(CancellationToken.None);
                await PersistStateUnsafeAsync(CancellationToken.None);
            }
            finally
            {
                _stateGate.Release();
            }

            await LogRunCompletionSummaryAsync("failed", CancellationToken.None);
            await _logger.ErrorAsync($"Archive run failed: {exception}", CancellationToken.None);
            return 1;
        }
    }

    private async Task LoadExistingStateAsync(CancellationToken cancellationToken)
    {
        _state = await JsonFileStore.ReadJsonAsync<ArchiveStateDocument>(_layout!.ArchiveStatePath, cancellationToken)
            ?? new ArchiveStateDocument();
    }

    private async Task LoadExistingIndexesAsync(CancellationToken cancellationToken)
    {
        CardsIndexDocument? cardsIndex = await JsonFileStore.ReadJsonAsync<CardsIndexDocument>(_layout!.CardsIndexPath, cancellationToken);

        if (cardsIndex is not null)
        {
            foreach (CardIndexEntryDocument card in cardsIndex.Cards)
            {
                _cardIndex[card.Id] = card;

                if (!_cardLanguages.TryGetValue(card.Id, out HashSet<string>? languages))
                {
                    languages = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    _cardLanguages[card.Id] = languages;
                }

                foreach (string language in card.Languages)
                {
                    languages.Add(language);
                }
            }
        }

        CardLanguagesIndexDocument? cardLanguagesIndex = await JsonFileStore.ReadJsonAsync<CardLanguagesIndexDocument>(_layout.CardLanguagesIndexPath, cancellationToken);

        if (cardLanguagesIndex is not null)
        {
            foreach (KeyValuePair<string, List<int>> languageEntry in cardLanguagesIndex.Languages)
            {
                foreach (int cardId in languageEntry.Value)
                {
                    if (!_cardLanguages.TryGetValue(cardId, out HashSet<string>? languages))
                    {
                        languages = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        _cardLanguages[cardId] = languages;
                    }

                    languages.Add(languageEntry.Key);
                }
            }
        }

        SetsIndexDocument? setsIndex = await JsonFileStore.ReadJsonAsync<SetsIndexDocument>(_layout.SetsIndexPath, cancellationToken);

        if (setsIndex is not null)
        {
            foreach (SetIndexEntryDocument set in setsIndex.Sets)
            {
                _setIndex[set.Key] = set;
            }
        }
    }

    private async Task<(List<NormalizedSetDefinition> SetDefinitions, DatasetStateDocument Dataset)> LoadSetDefinitionsAsync(
        CliOptions options,
        CancellationToken cancellationToken)
    {
        DatasetStateDocument dataset = await StartDatasetAsync("sets", cancellationToken);
        _consoleProgress?.SetActivity("Fetching set list");
        await _logger!.ProgressAsync("Fetching set list from YGOPRODeck...", cancellationToken);

        ApiResponse<List<CardSetListItemDto>> response = await _apiClient!.GetSetListAsync(cancellationToken);

        if (options.KeepRawPages)
        {
            await JsonFileStore.WriteTextIfChangedAsync(
                Path.Combine(_layout!.SourceApiDirectory, "cardsets.raw.json"),
                response.RawJson,
                cancellationToken);
        }

        Dictionary<string, List<CardSetListItemDto>> groupedBaseKeys = response.Value
            .Select(item => new
            {
                Item = item,
                BaseKey = ArchivePaths.CreateStableSetKey(item, 1)
            })
            .GroupBy(item => item.BaseKey, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.Select(item => item.Item).ToList(), StringComparer.OrdinalIgnoreCase);

        Dictionary<CardSetListItemDto, int> duplicateOrdinals = new(ReferenceEqualityComparer<CardSetListItemDto>.Default);

        foreach (List<CardSetListItemDto> group in groupedBaseKeys.Values)
        {
            for (int index = 0; index < group.Count; index++)
            {
                duplicateOrdinals[group[index]] = index + 1;
            }
        }

        List<NormalizedSetDefinition> normalizedSets = [.. response.Value
            .Select(item => new NormalizedSetDefinition
            {
                Key = ArchivePaths.CreateStableSetKey(item, duplicateOrdinals[item]),
                SetName = item.SetName,
                SetCode = item.SetCode,
                NumOfCards = item.NumOfCards,
                TcgDate = item.TcgDate,
                SetImageUrl = item.SetImage
            })
            .OrderBy(set => set.SetName, StringComparer.OrdinalIgnoreCase)];

        _setsByName.Clear();
        _setMembership.Clear();

        foreach (NormalizedSetDefinition set in normalizedSets)
        {
            if (!_setsByName.TryGetValue(set.SetName, out List<NormalizedSetDefinition>? setList))
            {
                setList = [];
                _setsByName[set.SetName] = setList;
            }

            setList.Add(set);
            _setMembership[set.Key] = [];
        }

        await _stateGate.WaitAsync(cancellationToken);

        try
        {
            dataset.PagesProcessed = 1;
            dataset.CompletedPageOffsets = [0];
            dataset.EntitiesSeen = normalizedSets.Count;
            await PersistStateUnsafeAsync(cancellationToken);
        }
        finally
        {
            _stateGate.Release();
        }

        _consoleProgress?.RegisterSetTotal(normalizedSets.Count);
        _consoleProgress?.SetActivity("Sets loaded");
        await _logger.ProgressAsync($"Loaded {normalizedSets.Count:N0} sets from the API.", cancellationToken);
        return (normalizedSets, dataset);
    }

    private async Task ArchiveTranslationsAsync(CliOptions options, CancellationToken cancellationToken)
    {
        ParallelOptions parallelOptions = new()
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = MaxConcurrentTranslationDatasets
        };

        await Parallel.ForEachAsync(
            Languages.Where(language => !string.Equals(language, "en", StringComparison.OrdinalIgnoreCase)),
            parallelOptions,
            async (language, innerCancellationToken) =>
            {
                await ArchiveCardDatasetAsync(
                    language,
                    options,
                    (card, _, cancellationTokenForCard) => ProcessTranslationCardAsync(card, language, cancellationTokenForCard),
                    innerCancellationToken);
            });
    }

    private async Task ArchiveCardDatasetAsync(
        string language,
        CliOptions options,
        Func<CardDto, CliOptions, CancellationToken, Task<CardArchiveOutcome>> cardProcessor,
        CancellationToken cancellationToken)
    {
        string datasetName = $"cards.{language}";
        DatasetStateDocument dataset = await StartDatasetAsync(datasetName, cancellationToken);
        int offset = 0;
        int pageNumber = 1;

        _consoleProgress?.SetActivity($"Cards {language}");
        await _logger!.ProgressAsync($"Starting {language} card archive...", cancellationToken);

        Task<ApiResponse<CardInfoPageDto>> currentPageTask = _apiClient!.GetCardPageAsync(language, options.PageSize, offset, cancellationToken);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ApiResponse<CardInfoPageDto> response = await currentPageTask;
            await LogCardPageStartAsync(datasetName, pageNumber, offset, response.Value, cancellationToken);

            bool hasNextPage = ShouldContinuePaging(response.Value.Meta, options, pageNumber);
            int nextOffset = response.Value.Meta?.NextPageOffset ?? 0;
            Task<ApiResponse<CardInfoPageDto>>? nextPageTask = hasNextPage
                ? _apiClient.GetCardPageAsync(language, options.PageSize, nextOffset, cancellationToken)
                : null;

            if (options.KeepRawPages)
            {
                await JsonFileStore.WriteTextIfChangedAsync(
                    ArchivePaths.BuildRawPagePath(_layout!.SourceApiDirectory, datasetName, pageNumber),
                    response.RawJson,
                    cancellationToken);
            }

            List<CardArchiveOutcome> outcomes = await ProcessCardPageAsync(response.Value.Data, language, options, cardProcessor, cancellationToken);
            await ApplyCardPageOutcomesAsync(dataset, pageNumber, offset, response.Value.Meta, outcomes, cancellationToken);

            if (!hasNextPage)
            {
                break;
            }

            offset = nextOffset;
            pageNumber++;
            currentPageTask = nextPageTask!;
        }

        await CompleteDatasetAsync(dataset, cancellationToken);
    }

    private static async Task<List<CardArchiveOutcome>> ProcessCardPageAsync(
        IReadOnlyCollection<CardDto> cards,
        string language,
        CliOptions options,
        Func<CardDto, CliOptions, CancellationToken, Task<CardArchiveOutcome>> cardProcessor,
        CancellationToken cancellationToken)
    {
        ConcurrentBag<CardArchiveOutcome> outcomes = [];
        ParallelOptions parallelOptions = new()
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = MaxConcurrentCardProcessors
        };

        await Parallel.ForEachAsync(
            cards,
            parallelOptions,
            async (card, innerCancellationToken) =>
            {
                try
                {
                    CardArchiveOutcome outcome = await cardProcessor(card, options, innerCancellationToken);
                    outcomes.Add(outcome);
                }
                catch (Exception exception)
                {
                    outcomes.Add(new CardArchiveOutcome
                    {
                        CardId = card.Id,
                        Language = language,
                        ErrorMessage = exception.Message,
                        EnglishName = string.Equals(language, "en", StringComparison.OrdinalIgnoreCase)
                            ? card.Name
                            : card.NameEn ?? card.Name
                    });
                }
            });

        return [.. outcomes.OrderBy(outcome => outcome.CardId)];
    }

    private async Task<CardArchiveOutcome> ProcessEnglishCardAsync(CardDto card, CliOptions options, CancellationToken cancellationToken)
    {
        string cardDirectory = ArchivePaths.EnsureEntityDirectory(_layout!.CardsDirectory, card.Id.ToString(CultureInfo.InvariantCulture), card.Name);
        CardImageArchiveResult imageResult = await ArchiveCardImagesAsync(cardDirectory, card.Id, card.Name, card.CardImages, options, cancellationToken);
        CardSetBuildResult cardSetBuildResult = BuildCardSetDocuments(card.CardSets);

        CanonicalCardDocument document = new()
        {
            Id = card.Id,
            KonamiId = card.KonamiId,
            Language = "en",
            Name = card.Name,
            Typeline = card.Typeline ?? [],
            Type = card.Type,
            HumanReadableCardType = card.HumanReadableCardType,
            FrameType = card.FrameType,
            Desc = card.Desc,
            PendDesc = card.PendDesc,
            MonsterDesc = card.MonsterDesc,
            Atk = card.Atk,
            Def = card.Def,
            Level = card.Level,
            Rank = card.Rank,
            Scale = card.Scale,
            LinkVal = card.LinkVal,
            LinkMarkers = card.LinkMarkers ?? [],
            Race = card.Race,
            Attribute = card.Attribute,
            Archetype = card.Archetype,
            YgoprodeckUrl = card.YgoprodeckUrl,
            BanlistInfo = card.BanlistInfo is null
                ? null
                : new BanlistInfoDocument
                {
                    BanTcg = card.BanlistInfo.BanTcg,
                    BanOcg = card.BanlistInfo.BanOcg,
                    BanGoat = card.BanlistInfo.BanGoat,
                    BanEdison = card.BanlistInfo.BanEdison
                },
            CardSets = cardSetBuildResult.Documents,
            CardImages = imageResult.Documents
        };

        bool wroteJson = await JsonFileStore.WriteJsonIfChangedAsync(Path.Combine(cardDirectory, "card.en.json"), document, cancellationToken);

        return new CardArchiveOutcome
        {
            CardId = card.Id,
            Language = "en",
            WroteFiles = wroteJson || imageResult.WroteAnyFiles,
            EnglishName = card.Name,
            KonamiId = card.KonamiId,
            CardDirectory = cardDirectory,
            SetKeys = cardSetBuildResult.SetKeys
        };
    }

    private async Task<CardArchiveOutcome> ProcessTranslationCardAsync(CardDto card, string language, CancellationToken cancellationToken)
    {
        string cardDirectory = ResolveCardDirectory(card.Id, card.NameEn ?? card.Name);
        CardTranslationDocument document = new()
        {
            Id = card.Id,
            Language = language,
            NameEn = card.NameEn,
            Name = card.Name,
            Desc = card.Desc,
            PendDesc = card.PendDesc,
            MonsterDesc = card.MonsterDesc,
            YgoprodeckUrl = card.YgoprodeckUrl
        };

        bool wroteJson = await JsonFileStore.WriteJsonIfChangedAsync(Path.Combine(cardDirectory, $"card.{language}.json"), document, cancellationToken);

        return new CardArchiveOutcome
        {
            CardId = card.Id,
            Language = language,
            WroteFiles = wroteJson,
            EnglishName = card.NameEn ?? card.Name,
            KonamiId = card.KonamiId,
            CardDirectory = cardDirectory,
            SetKeys = []
        };
    }

    private async Task ApplyCardPageOutcomesAsync(
        DatasetStateDocument dataset,
        int pageNumber,
        int offset,
        CardInfoMetaDto? meta,
        IReadOnlyCollection<CardArchiveOutcome> outcomes,
        CancellationToken cancellationToken)
    {
        string summaryMessage;
        List<string> failureMessages = [];

        await _stateGate.WaitAsync(cancellationToken);

        try
        {
            dataset.PagesProcessed++;
            dataset.CompletedPageOffsets.Add(offset);

            foreach (CardArchiveOutcome outcome in outcomes)
            {
                dataset.EntitiesSeen++;

                if (string.IsNullOrWhiteSpace(outcome.ErrorMessage))
                {
                    if (outcome.WroteFiles)
                    {
                        dataset.EntitiesWritten++;
                    }
                    else
                    {
                        dataset.EntitiesSkipped++;
                    }

                    if (!string.IsNullOrWhiteSpace(outcome.EnglishName) && !string.IsNullOrWhiteSpace(outcome.CardDirectory))
                    {
                        UpdateCardIndex(outcome.CardId, outcome.EnglishName!, outcome.KonamiId, outcome.CardDirectory!, outcome.Language);
                    }

                    foreach (string setKey in outcome.SetKeys)
                    {
                        if (_setMembership.TryGetValue(setKey, out HashSet<int>? cardIds))
                        {
                            cardIds.Add(outcome.CardId);
                        }
                    }

                    ClearFailureUnsafe($"card:{outcome.Language}:{outcome.CardId}");
                }
                else
                {
                    dataset.FailedCount++;
                    RecordFailureUnsafe($"card:{outcome.Language}:{outcome.CardId}", dataset.Name, outcome.ErrorMessage);
                    failureMessages.Add(BuildCardFailureLogMessage(dataset.Name, outcome));
                }
            }

            summaryMessage = BuildCardPageSummary(dataset, pageNumber, meta);
            await PersistStateUnsafeAsync(cancellationToken);
        }
        finally
        {
            _stateGate.Release();
        }

        foreach (string failureMessage in failureMessages)
        {
            await _logger!.ErrorAsync(failureMessage, cancellationToken);
        }

        await _logger!.ProgressAsync(summaryMessage, cancellationToken);
        _consoleProgress?.AdvanceOverall(outcomes.Count, summaryMessage);
    }

    private async Task CompleteDatasetAsync(DatasetStateDocument dataset, CancellationToken cancellationToken)
    {
        await _stateGate.WaitAsync(cancellationToken);

        try
        {
            dataset.LastCompletedUtc = DateTimeOffset.UtcNow;
            dataset.Succeeded = true;
            await PersistStateUnsafeAsync(cancellationToken);
        }
        finally
        {
            _stateGate.Release();
        }

        await LogDatasetCompletedAsync(dataset, cancellationToken);
    }

    private async Task WriteSetFilesAsync(
        DatasetStateDocument dataset,
        List<NormalizedSetDefinition> setDefinitions,
        CliOptions options,
        CancellationToken cancellationToken)
    {
        _consoleProgress?.RegisterSetTotal(setDefinitions.Count);
        _consoleProgress?.SetActivity("Writing set folders");
        await _logger!.ProgressAsync($"Writing {setDefinitions.Count:N0} set folders...", cancellationToken);

        int processedSets = 0;
        ParallelOptions parallelOptions = new()
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = MaxConcurrentSetProcessors
        };

        await Parallel.ForEachAsync(
            setDefinitions,
            parallelOptions,
            async (set, innerCancellationToken) =>
            {
                SetArchiveOutcome outcome = await ProcessSetAsync(set, options, innerCancellationToken);
                int processedSetCount = Interlocked.Increment(ref processedSets);
                string? progressMessage = null;

                await _stateGate.WaitAsync(innerCancellationToken);

                try
                {
                    if (string.IsNullOrWhiteSpace(outcome.ErrorMessage))
                    {
                        if (outcome.IndexEntry is not null)
                        {
                            _setIndex[outcome.IndexEntry.Key] = outcome.IndexEntry;
                        }

                        if (outcome.WroteFiles)
                        {
                            dataset.EntitiesWritten++;
                        }
                        else
                        {
                            dataset.EntitiesSkipped++;
                        }

                        ClearFailureUnsafe($"set:{outcome.Key}");
                    }
                    else
                    {
                        dataset.FailedCount++;
                        RecordFailureUnsafe($"set:{outcome.Key}", dataset.Name, outcome.ErrorMessage);
                    }

                    if (processedSetCount == 1 || processedSetCount % SetProgressLogInterval == 0 || processedSetCount == setDefinitions.Count)
                    {
                        progressMessage = BuildSetProgressMessage(processedSetCount, setDefinitions.Count, dataset);
                        await PersistStateUnsafeAsync(innerCancellationToken);
                    }
                }
                finally
                {
                    _stateGate.Release();
                }

                if (!string.IsNullOrWhiteSpace(progressMessage))
                {
                    await _logger.ProgressAsync(progressMessage, innerCancellationToken);
                }

                _consoleProgress?.AdvanceOverall(1, $"sets {processedSetCount:N0}/{setDefinitions.Count:N0}");

                if (!string.IsNullOrWhiteSpace(outcome.ErrorMessage))
                {
                    await _logger.ErrorAsync(BuildSetFailureLogMessage(dataset.Name, outcome), innerCancellationToken);
                }
            });

        await CompleteDatasetAsync(dataset, cancellationToken);
    }

    private async Task<SetArchiveOutcome> ProcessSetAsync(NormalizedSetDefinition set, CliOptions options, CancellationToken cancellationToken)
    {
        try
        {
            string setDirectory = ArchivePaths.EnsureEntityDirectory(_layout!.SetsDirectory, set.Key, set.SetName);
            if (NeedsArchiveRemoteFile(setDirectory, "image", null, set.SetImageUrl, options))
            {
                await _logger!.ProgressAsync($"Downloading set assets for {set.Key} - {set.SetName}", cancellationToken);
            }

            ArchivedFileResult setImageResult = await ArchiveRemoteFileAsync(
                setDirectory,
                "image",
                null,
                set.SetImageUrl,
                $"set {set.Key} - {set.SetName} image",
                options,
                cancellationToken);

            string relativeSetDirectory = ArchivePaths.GetRelativePath(_layout.RootDirectory, setDirectory);

            SetDocument setDocument = new()
            {
                Key = set.Key,
                SetName = set.SetName,
                SetCode = set.SetCode,
                NumOfCards = set.NumOfCards,
                TcgDate = set.TcgDate,
                SetImageUrl = set.SetImageUrl,
                ArchivedImagePath = setImageResult.RelativePath
            };

            SetCardsDocument cardsDocument = new()
            {
                SetKey = set.Key,
                SetName = set.SetName,
                Cards = _setMembership.TryGetValue(set.Key, out HashSet<int>? cards)
                    ? [.. cards.Order()]
                    : []
            };

            bool wroteSetJson = await JsonFileStore.WriteJsonIfChangedAsync(Path.Combine(setDirectory, "set.json"), setDocument, cancellationToken);
            bool wroteCardsJson = await JsonFileStore.WriteJsonIfChangedAsync(Path.Combine(setDirectory, "cards.json"), cardsDocument, cancellationToken);

            return new SetArchiveOutcome
            {
                Key = set.Key,
                WroteFiles = wroteSetJson || wroteCardsJson || setImageResult.WroteFile,
                IndexEntry = new SetIndexEntryDocument
                {
                    Key = set.Key,
                    SetName = set.SetName,
                    SetCode = set.SetCode,
                    NumOfCards = set.NumOfCards,
                    TcgDate = set.TcgDate,
                    RelativePath = relativeSetDirectory
                }
            };
        }
        catch (Exception exception)
        {
            return new SetArchiveOutcome
            {
                Key = set.Key,
                SetName = set.SetName,
                ErrorMessage = exception.Message
            };
        }
    }

    private async Task<CardImageArchiveResult> ArchiveCardImagesAsync(
        string cardDirectory,
        int cardId,
        string cardName,
        List<CardImageDto>? rawImages,
        CliOptions options,
        CancellationToken cancellationToken)
    {
        if (rawImages is null || rawImages.Count == 0)
        {
            return new CardImageArchiveResult([], false);
        }

        List<CardImageDto> uniqueImages = [.. rawImages
            .Where(image =>
                !string.IsNullOrWhiteSpace(image.ImageUrl)
                || !string.IsNullOrWhiteSpace(image.ImageUrlSmall)
                || !string.IsNullOrWhiteSpace(image.ImageUrlCropped))
            .GroupBy(
                image => $"{image.ImageUrl ?? string.Empty}|{image.ImageUrlSmall ?? string.Empty}|{image.ImageUrlCropped ?? string.Empty}",
                StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())];

        if (uniqueImages.Count == 0)
        {
            return new CardImageArchiveResult([], false);
        }

        string imagesDirectory = Path.Combine(cardDirectory, "images");
        Directory.CreateDirectory(imagesDirectory);

        bool wroteAnyFiles = false;
        List<ArchivedCardImageDocument> documents = new(uniqueImages.Count);

        for (int index = 0; index < uniqueImages.Count; index++)
        {
            CardImageDto image = uniqueImages[index];
            bool useIndexedNames = uniqueImages.Count > 1;
            int? variantIndex = useIndexedNames ? index + 1 : null;

            if (NeedsArchiveRemoteFile(imagesDirectory, "full", variantIndex, image.ImageUrl, options)
                || NeedsArchiveRemoteFile(imagesDirectory, "small", variantIndex, image.ImageUrlSmall, options)
                || NeedsArchiveRemoteFile(imagesDirectory, "art-cropped", variantIndex, image.ImageUrlCropped, options))
            {
                string imagePart = uniqueImages.Count > 1
                    ? $"image set {index + 1:N0}/{uniqueImages.Count:N0}"
                    : "image set";

                await _logger!.ProgressAsync($"Downloading card assets for {cardId} - {cardName} ({imagePart})", cancellationToken);
            }

            string assetLabelPrefix = $"card {cardId}";

            Task<ArchivedFileResult> fullTask = ArchiveRemoteFileAsync(
                imagesDirectory,
                "full",
                variantIndex,
                image.ImageUrl,
                $"{assetLabelPrefix} full",
                options,
                cancellationToken);

            Task<ArchivedFileResult> smallTask = ArchiveRemoteFileAsync(
                imagesDirectory,
                "small",
                variantIndex,
                image.ImageUrlSmall,
                $"{assetLabelPrefix} small",
                options,
                cancellationToken);

            Task<ArchivedFileResult> croppedTask = ArchiveRemoteFileAsync(
                imagesDirectory,
                "art-cropped",
                variantIndex,
                image.ImageUrlCropped,
                $"{assetLabelPrefix} art-cropped",
                options,
                cancellationToken);

            ArchivedFileResult[] results = await Task.WhenAll(fullTask, smallTask, croppedTask);
            ArchivedFileResult fullResult = results[0];
            ArchivedFileResult smallResult = results[1];
            ArchivedFileResult croppedResult = results[2];

            wroteAnyFiles |= fullResult.WroteFile || smallResult.WroteFile || croppedResult.WroteFile;

            documents.Add(new ArchivedCardImageDocument
            {
                Order = index + 1,
                SourceId = image.Id,
                ImageUrl = image.ImageUrl,
                ImageUrlSmall = image.ImageUrlSmall,
                ImageUrlCropped = image.ImageUrlCropped,
                ArchivedFullPath = fullResult.RelativePath is null ? null : Path.Combine("images", fullResult.RelativePath),
                ArchivedSmallPath = smallResult.RelativePath is null ? null : Path.Combine("images", smallResult.RelativePath),
                ArchivedCroppedPath = croppedResult.RelativePath is null ? null : Path.Combine("images", croppedResult.RelativePath)
            });
        }

        return new CardImageArchiveResult(documents, wroteAnyFiles);
    }

    private async Task<ArchivedFileResult> ArchiveRemoteFileAsync(
        string destinationDirectory,
        string baseFileName,
        int? variantIndex,
        string? sourceUrl,
        string assetLabel,
        CliOptions options,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(sourceUrl))
        {
            return new ArchivedFileResult(null, false);
        }

        string destinationPath = BuildArchiveRemoteFilePath(destinationDirectory, baseFileName, variantIndex, sourceUrl);

        if (!options.ForceRefresh && File.Exists(destinationPath) && new FileInfo(destinationPath).Length > 0)
        {
            return new ArchivedFileResult(Path.GetFileName(destinationPath), false);
        }

        await _downloadLimiter.WaitAsync(cancellationToken);

        try
        {
            _consoleProgress?.DownloadStarted(assetLabel);

            try
            {
                await _apiClient!.DownloadFileAsync(sourceUrl, destinationPath, cancellationToken);
            }
            catch (HttpRequestException exception) when (exception.StatusCode is HttpStatusCode.NotFound)
            {
                await _logger!.WarnAsync($"Missing remote file - {assetLabel}: {sourceUrl} returned 404; skipping.", cancellationToken);

                if (File.Exists(destinationPath) && new FileInfo(destinationPath).Length > 0)
                {
                    return new ArchivedFileResult(Path.GetFileName(destinationPath), false);
                }

                return new ArchivedFileResult(null, false);
            }
        }
        finally
        {
            _consoleProgress?.DownloadCompleted(assetLabel);
            _downloadLimiter.Release();
        }

        return new ArchivedFileResult(Path.GetFileName(destinationPath), true);
    }

    private static bool NeedsArchiveRemoteFile(
        string destinationDirectory,
        string baseFileName,
        int? variantIndex,
        string? sourceUrl,
        CliOptions options)
    {
        if (string.IsNullOrWhiteSpace(sourceUrl))
        {
            return false;
        }

        string destinationPath = BuildArchiveRemoteFilePath(destinationDirectory, baseFileName, variantIndex, sourceUrl);
        return options.ForceRefresh || !File.Exists(destinationPath) || new FileInfo(destinationPath).Length == 0;
    }

    private static string BuildArchiveRemoteFilePath(string destinationDirectory, string baseFileName, int? variantIndex, string sourceUrl)
    {
        string extension = ArchivePaths.GetImageFileExtension(sourceUrl);
        string suffix = variantIndex is int index ? $"-{index:00}" : string.Empty;
        string fileName = $"{baseFileName}{suffix}{extension}";
        return Path.Combine(destinationDirectory, fileName);
    }

    private CardSetBuildResult BuildCardSetDocuments(List<CardSetPrintDto>? rawPrints)
    {
        if (rawPrints is null || rawPrints.Count == 0)
        {
            return new CardSetBuildResult([], []);
        }

        List<CardSetPrintDocument> documents = new(rawPrints.Count);
        HashSet<string> setKeys = new(StringComparer.OrdinalIgnoreCase);

        foreach (CardSetPrintDto rawPrint in rawPrints)
        {
            string? setKey = ResolveSetKey(rawPrint);

            if (!string.IsNullOrWhiteSpace(setKey))
            {
                setKeys.Add(setKey);
            }

            documents.Add(new CardSetPrintDocument
            {
                SetName = rawPrint.SetName,
                SetKey = setKey,
                SetCode = rawPrint.SetCode,
                SetRarity = rawPrint.SetRarity,
                SetRarityCode = rawPrint.SetRarityCode
            });
        }

        return new CardSetBuildResult(documents, [.. setKeys.OrderBy(key => key, StringComparer.OrdinalIgnoreCase)]);
    }

    private string? ResolveSetKey(CardSetPrintDto rawPrint)
    {
        if (!_setsByName.TryGetValue(rawPrint.SetName, out List<NormalizedSetDefinition>? candidates) || candidates.Count == 0)
        {
            return null;
        }

        if (candidates.Count == 1)
        {
            return candidates[0].Key;
        }

        string? printCodePrefix = rawPrint.SetCode?
            .Split('-', 2, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault();

        if (!string.IsNullOrWhiteSpace(printCodePrefix))
        {
            List<NormalizedSetDefinition> exactMatch = [.. candidates.Where(candidate => string.Equals(candidate.SetCode, printCodePrefix, StringComparison.OrdinalIgnoreCase))];

            if (exactMatch.Count == 1)
            {
                return exactMatch[0].Key;
            }
        }

        return candidates[0].Key;
    }

    private string ResolveCardDirectory(int cardId, string preferredEnglishName)
    {
        return ArchivePaths.FindEntityDirectory(_layout!.CardsDirectory, cardId.ToString(CultureInfo.InvariantCulture))
            ?? ArchivePaths.EnsureEntityDirectory(_layout.CardsDirectory, cardId.ToString(CultureInfo.InvariantCulture), preferredEnglishName);
    }

    private void UpdateCardIndex(int cardId, string englishName, int? konamiId, string cardDirectory, string language)
    {
        if (!_cardIndex.TryGetValue(cardId, out CardIndexEntryDocument? cardEntry))
        {
            cardEntry = new CardIndexEntryDocument
            {
                Id = cardId,
                KonamiId = konamiId,
                Name = englishName,
                RelativePath = ArchivePaths.GetRelativePath(_layout!.RootDirectory, cardDirectory)
            };

            _cardIndex[cardId] = cardEntry;
        }
        else
        {
            cardEntry.Name = englishName;
            cardEntry.RelativePath = ArchivePaths.GetRelativePath(_layout!.RootDirectory, cardDirectory);
        }

        if (!_cardLanguages.TryGetValue(cardId, out HashSet<string>? languages))
        {
            languages = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _cardLanguages[cardId] = languages;
        }

        languages.Add(language);
        cardEntry.Languages.Clear();

        foreach (string currentLanguage in languages.OrderBy(value => value, StringComparer.OrdinalIgnoreCase))
        {
            cardEntry.Languages.Add(currentLanguage);
        }
    }

    private async Task<DatasetStateDocument> StartDatasetAsync(string datasetName, CancellationToken cancellationToken)
    {
        await _stateGate.WaitAsync(cancellationToken);

        try
        {
            DatasetStateDocument dataset = StartDatasetUnsafe(datasetName);
            await PersistStateUnsafeAsync(cancellationToken);
            return dataset;
        }
        finally
        {
            _stateGate.Release();
        }
    }

    private DatasetStateDocument StartDatasetUnsafe(string datasetName)
    {
        if (!_state.Datasets.TryGetValue(datasetName, out DatasetStateDocument? dataset))
        {
            dataset = new DatasetStateDocument
            {
                Name = datasetName
            };

            _state.Datasets[datasetName] = dataset;
        }

        dataset.LastStartedUtc = DateTimeOffset.UtcNow;
        dataset.LastCompletedUtc = null;
        dataset.PagesProcessed = 0;
        dataset.CompletedPageOffsets = [];
        dataset.EntitiesSeen = 0;
        dataset.EntitiesWritten = 0;
        dataset.EntitiesSkipped = 0;
        dataset.FailedCount = 0;
        dataset.Succeeded = false;
        return dataset;
    }

    private void RecordFailureUnsafe(string itemKey, string dataset, string errorMessage)
    {
        if (!_state.FailedItems.TryGetValue(itemKey, out FailureStateDocument? failure))
        {
            failure = new FailureStateDocument
            {
                Dataset = dataset,
                ItemKey = itemKey,
                LastError = errorMessage,
                RetryCount = 0,
                LastSeenUtc = DateTimeOffset.UtcNow
            };

            _state.FailedItems[itemKey] = failure;
        }

        failure.RetryCount++;
        failure.LastError = errorMessage;
        failure.LastSeenUtc = DateTimeOffset.UtcNow;
    }

    private void ClearFailureUnsafe(string itemKey)
    {
        _state.FailedItems.Remove(itemKey);
    }

    private Task<bool> PersistStateUnsafeAsync(CancellationToken cancellationToken)
    {
        return JsonFileStore.WriteJsonIfChangedAsync(_layout!.ArchiveStatePath, _state, cancellationToken);
    }

    private async Task PersistIndexesAsync(CancellationToken cancellationToken)
    {
        await _stateGate.WaitAsync(cancellationToken);

        try
        {
            await PersistIndexesUnsafeAsync(cancellationToken);
        }
        finally
        {
            _stateGate.Release();
        }
    }

    private async Task PersistIndexesUnsafeAsync(CancellationToken cancellationToken)
    {
        CardsIndexDocument cardsIndexDocument = new()
        {
            GeneratedUtc = DateTimeOffset.UtcNow,
            Cards = [.. _cardIndex.Values.OrderBy(card => card.Id)]
        };

        CardLanguagesIndexDocument cardLanguagesDocument = new()
        {
            GeneratedUtc = DateTimeOffset.UtcNow,
            Languages = _cardLanguages
                .SelectMany(entry => entry.Value.Select(language => new { Language = language, CardId = entry.Key }))
                .GroupBy(entry => entry.Language, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    group => group.Key,
                    group => group.Select(entry => entry.CardId).Order().ToList(),
                    StringComparer.OrdinalIgnoreCase)
        };

        SetsIndexDocument setsIndexDocument = new()
        {
            GeneratedUtc = DateTimeOffset.UtcNow,
            Sets = [.. _setIndex.Values.OrderBy(set => set.Key, StringComparer.OrdinalIgnoreCase)]
        };

        await JsonFileStore.WriteJsonIfChangedAsync(_layout!.CardsIndexPath, cardsIndexDocument, cancellationToken);
        await JsonFileStore.WriteJsonIfChangedAsync(_layout.CardLanguagesIndexPath, cardLanguagesDocument, cancellationToken);
        await JsonFileStore.WriteJsonIfChangedAsync(_layout.SetsIndexPath, setsIndexDocument, cancellationToken);
    }

    private async Task LogCardPageStartAsync(string datasetName, int pageNumber, int offset, CardInfoPageDto page, CancellationToken cancellationToken)
    {
        string pagePart = page.Meta?.TotalPages is int totalPages
            ? $"{pageNumber:N0}/{totalPages:N0}"
            : pageNumber.ToString("N0", CultureInfo.InvariantCulture);

        if (page.Meta?.TotalRows is int totalRows && totalRows > 0)
        {
            _consoleProgress?.RegisterDatasetTotal(datasetName, totalRows);
            int start = Math.Min(totalRows, offset + 1);
            int end = Math.Min(totalRows, offset + page.Data.Count);
            _consoleProgress?.SetActivity($"{datasetName} page {pagePart}");
            await _logger!.ProgressAsync($"{datasetName} page {pagePart} - processing cards {start:N0}-{end:N0} of {totalRows:N0}", cancellationToken);
            return;
        }

        _consoleProgress?.SetActivity($"{datasetName} page {pagePart}");
        await _logger!.ProgressAsync($"{datasetName} page {pagePart} - processing {page.Data.Count:N0} cards", cancellationToken);
    }

    private static string BuildCardPageSummary(DatasetStateDocument dataset, int pageNumber, CardInfoMetaDto? meta)
    {
        string pagePart = meta?.TotalPages is int totalPages
            ? $"{pageNumber:N0}/{totalPages:N0}"
            : pageNumber.ToString("N0", CultureInfo.InvariantCulture);

        string progressPart = $"{dataset.EntitiesSeen:N0} cards";
        string remainingPart = string.Empty;

        if (meta?.TotalRows is int totalRows && totalRows > 0)
        {
            string percent = ((double)dataset.EntitiesSeen / totalRows).ToString("P1", CultureInfo.InvariantCulture);
            progressPart = $"{dataset.EntitiesSeen:N0}/{totalRows:N0} cards ({percent})";
            remainingPart = $", remaining {Math.Max(0, totalRows - dataset.EntitiesSeen):N0}";
        }

        return $"{dataset.Name} page {pagePart} complete - {progressPart}{remainingPart}; written {dataset.EntitiesWritten:N0}, skipped {dataset.EntitiesSkipped:N0}, failed {dataset.FailedCount:N0}";
    }

    private static string BuildSetProgressMessage(int processedSets, int totalSets, DatasetStateDocument dataset)
    {
        string percent = totalSets > 0
            ? ((double)processedSets / totalSets).ToString("P1", CultureInfo.InvariantCulture)
            : "0.0 %";

        return $"sets progress - {processedSets:N0}/{totalSets:N0} ({percent}); written {dataset.EntitiesWritten:N0}, skipped {dataset.EntitiesSkipped:N0}, failed {dataset.FailedCount:N0}";
    }

    private static string BuildCardFailureLogMessage(string datasetName, CardArchiveOutcome outcome)
    {
        string namePart = string.IsNullOrWhiteSpace(outcome.EnglishName)
            ? string.Empty
            : $" - {outcome.EnglishName}";

        return $"Failed {datasetName} card {outcome.CardId}{namePart}: {outcome.ErrorMessage}";
    }

    private static string BuildSetFailureLogMessage(string datasetName, SetArchiveOutcome outcome)
    {
        string namePart = string.IsNullOrWhiteSpace(outcome.SetName)
            ? string.Empty
            : $" - {outcome.SetName}";

        return $"Failed {datasetName} set {outcome.Key}{namePart}: {outcome.ErrorMessage}";
    }

    private async Task LogDatasetCompletedAsync(DatasetStateDocument dataset, CancellationToken cancellationToken)
    {
        await _logger!.ProgressAsync(
            $"{dataset.Name} complete - seen {dataset.EntitiesSeen:N0}, written {dataset.EntitiesWritten:N0}, skipped {dataset.EntitiesSkipped:N0}, failed {dataset.FailedCount:N0}",
            cancellationToken);
    }

    private async Task LogRunStartSummaryAsync(CancellationToken cancellationToken)
    {
        int retryItemCount = _state.FailedItems.Count(item => !string.Equals(item.Key, "run", StringComparison.OrdinalIgnoreCase));

        await _logger!.InfoAsync(
            $"Run summary - datasets {string.Join(", ", Languages)}; card workers {MaxConcurrentCardProcessors}; set workers {MaxConcurrentSetProcessors}; translation workers {MaxConcurrentTranslationDatasets}; download workers {MaxConcurrentDownloads}",
            cancellationToken);

        if (retryItemCount > 0)
        {
            await _logger.InfoAsync($"Retry summary - {retryItemCount:N0} previously failed item(s) will be retried in this run.", cancellationToken);
        }
    }

    private async Task LogRunCompletionSummaryAsync(string status, CancellationToken cancellationToken)
    {
        await _stateGate.WaitAsync(cancellationToken);

        try
        {
            int totalSeen = _state.Datasets.Values.Sum(dataset => dataset.EntitiesSeen);
            int totalWritten = _state.Datasets.Values.Sum(dataset => dataset.EntitiesWritten);
            int totalSkipped = _state.Datasets.Values.Sum(dataset => dataset.EntitiesSkipped);
            int totalFailed = _state.Datasets.Values.Sum(dataset => dataset.FailedCount);
            int remainingFailedItems = _state.FailedItems.Count(item => !string.Equals(item.Key, "run", StringComparison.OrdinalIgnoreCase));

            await _logger!.InfoAsync(
                $"Run summary - status {status}; seen {totalSeen:N0}; written {totalWritten:N0}; skipped {totalSkipped:N0}; failed {totalFailed:N0}; pending retries {remainingFailedItems:N0}",
                cancellationToken);

            string datasetSummary = string.Join(
                " | ",
                _state.Datasets.Values
                    .OrderBy(dataset => dataset.Name, StringComparer.OrdinalIgnoreCase)
                    .Select(dataset => $"{dataset.Name}: w{dataset.EntitiesWritten:N0}/s{dataset.EntitiesSkipped:N0}/f{dataset.FailedCount:N0}"));

            await _logger.InfoAsync($"Dataset summary - {datasetSummary}", cancellationToken);
        }
        finally
        {
            _stateGate.Release();
        }
    }

    private static bool ShouldContinuePaging(CardInfoMetaDto? meta, CliOptions options, int pageNumber)
    {
        if (options.MaxPages is not null && pageNumber >= options.MaxPages.Value)
        {
            return false;
        }

        return meta?.NextPageOffset is not null;
    }

    private sealed record CardSetBuildResult(List<CardSetPrintDocument> Documents, List<string> SetKeys);

    private sealed record CardImageArchiveResult(List<ArchivedCardImageDocument> Documents, bool WroteAnyFiles);

    private sealed record ArchivedFileResult(string? RelativePath, bool WroteFile);

    private sealed class CardArchiveOutcome
    {
        public required int CardId { get; init; }

        public required string Language { get; init; }

        public bool WroteFiles { get; init; }

        public string? ErrorMessage { get; init; }

        public string? EnglishName { get; init; }

        public int? KonamiId { get; init; }

        public string? CardDirectory { get; init; }

        public IReadOnlyList<string> SetKeys { get; init; } = [];
    }

    private sealed class SetArchiveOutcome
    {
        public required string Key { get; init; }

        public string? SetName { get; init; }

        public bool WroteFiles { get; init; }

        public string? ErrorMessage { get; init; }

        public SetIndexEntryDocument? IndexEntry { get; init; }
    }
}

internal sealed class ReferenceEqualityComparer<T> : IEqualityComparer<T>
    where T : class
{
    public static ReferenceEqualityComparer<T> Default { get; } = new();

    public bool Equals(T? x, T? y)
    {
        return ReferenceEquals(x, y);
    }

    public int GetHashCode(T obj)
    {
        return RuntimeHelpers.GetHashCode(obj);
    }
}

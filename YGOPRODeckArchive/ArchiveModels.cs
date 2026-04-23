namespace YGOPRODeckArchive;

internal sealed class ArchiveLayout
{
    public required string RootDirectory { get; init; }

    public required string ManifestDirectory { get; init; }

    public required string RunHistoryDirectory { get; init; }

    public required string SourceApiDirectory { get; init; }

    public required string CardsDirectory { get; init; }

    public required string SetsDirectory { get; init; }

    public required string LogsDirectory { get; init; }

    public required string LatestLogPath { get; init; }

    public required string ArchiveStatePath { get; init; }

    public required string CardsIndexPath { get; init; }

    public required string CardLanguagesIndexPath { get; init; }

    public required string SetsIndexPath { get; init; }
}

internal sealed class NormalizedSetDefinition
{
    public required string Key { get; init; }

    public required string SetName { get; init; }

    public string? SetCode { get; init; }

    public int? NumOfCards { get; init; }

    public string? TcgDate { get; init; }

    public string? SetImageUrl { get; init; }
}

internal sealed class CanonicalCardDocument
{
    public required int Id { get; init; }

    public int? KonamiId { get; init; }

    public required string Language { get; init; }

    public required string Name { get; init; }

    public List<string> Typeline { get; init; } = [];

    public string? Type { get; init; }

    public string? HumanReadableCardType { get; init; }

    public string? FrameType { get; init; }

    public string? Desc { get; init; }

    public string? PendDesc { get; init; }

    public string? MonsterDesc { get; init; }

    public int? Atk { get; init; }

    public int? Def { get; init; }

    public int? Level { get; init; }

    public int? Rank { get; init; }

    public int? Scale { get; init; }

    public int? LinkVal { get; init; }

    public List<string> LinkMarkers { get; init; } = [];

    public string? Race { get; init; }

    public string? Attribute { get; init; }

    public string? Archetype { get; init; }

    public string? YgoprodeckUrl { get; init; }

    public BanlistInfoDocument? BanlistInfo { get; init; }

    public List<CardSetPrintDocument> CardSets { get; init; } = [];

    public List<ArchivedCardImageDocument> CardImages { get; init; } = [];
}

internal sealed class CardTranslationDocument
{
    public required int Id { get; init; }

    public required string Language { get; init; }

    public string? NameEn { get; init; }

    public required string Name { get; init; }

    public string? Desc { get; init; }

    public string? PendDesc { get; init; }

    public string? MonsterDesc { get; init; }

    public string? YgoprodeckUrl { get; init; }
}

internal sealed class BanlistInfoDocument
{
    public string? BanTcg { get; init; }

    public string? BanOcg { get; init; }

    public string? BanGoat { get; init; }

    public string? BanEdison { get; init; }
}

internal sealed class CardSetPrintDocument
{
    public required string SetName { get; init; }

    public string? SetKey { get; init; }

    public string? SetCode { get; init; }

    public string? SetRarity { get; init; }

    public string? SetRarityCode { get; init; }
}

internal sealed class ArchivedCardImageDocument
{
    public required int Order { get; init; }

    public int? SourceId { get; init; }

    public string? ImageUrl { get; init; }

    public string? ImageUrlSmall { get; init; }

    public string? ImageUrlCropped { get; init; }

    public string? ArchivedFullPath { get; init; }

    public string? ArchivedSmallPath { get; init; }

    public string? ArchivedCroppedPath { get; init; }
}

internal sealed class SetDocument
{
    public required string Key { get; init; }

    public required string SetName { get; init; }

    public string? SetCode { get; init; }

    public int? NumOfCards { get; init; }

    public string? TcgDate { get; init; }

    public string? SetImageUrl { get; init; }

    public string? ArchivedImagePath { get; init; }
}

internal sealed class SetCardsDocument
{
    public required string SetKey { get; init; }

    public required string SetName { get; init; }

    public List<int> Cards { get; init; } = [];
}

internal sealed class CardsIndexDocument
{
    public DateTimeOffset GeneratedUtc { get; init; }

    public List<CardIndexEntryDocument> Cards { get; init; } = [];
}

internal sealed class CardIndexEntryDocument
{
    public required int Id { get; init; }

    public int? KonamiId { get; init; }

    public required string Name { get; set; }

    public required string RelativePath { get; set; }

    public HashSet<string> Languages { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}

internal sealed class CardLanguagesIndexDocument
{
    public DateTimeOffset GeneratedUtc { get; init; }

    public Dictionary<string, List<int>> Languages { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}

internal sealed class SetsIndexDocument
{
    public DateTimeOffset GeneratedUtc { get; init; }

    public List<SetIndexEntryDocument> Sets { get; init; } = [];
}

internal sealed class SetIndexEntryDocument
{
    public required string Key { get; init; }

    public required string SetName { get; set; }

    public string? SetCode { get; set; }

    public int? NumOfCards { get; set; }

    public string? TcgDate { get; set; }

    public string? RelativePath { get; set; }
}

internal sealed class ArchiveStateDocument
{
    public int SchemaVersion { get; set; } = 1;

    public string? AppVersion { get; set; }

    public string? ArchiveRoot { get; set; }

    public DateTimeOffset? LastRunStartedUtc { get; set; }

    public DateTimeOffset? LastRunCompletedUtc { get; set; }

    public string? LastRunStatus { get; set; }

    public Dictionary<string, DatasetStateDocument> Datasets { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    public Dictionary<string, FailureStateDocument> FailedItems { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

internal sealed class DatasetStateDocument
{
    public required string Name { get; init; }

    public DateTimeOffset? LastStartedUtc { get; set; }

    public DateTimeOffset? LastCompletedUtc { get; set; }

    public int PagesProcessed { get; set; }

    public List<int> CompletedPageOffsets { get; set; } = [];

    public int EntitiesSeen { get; set; }

    public int EntitiesWritten { get; set; }

    public int EntitiesSkipped { get; set; }

    public int FailedCount { get; set; }

    public bool Succeeded { get; set; }
}

internal sealed class FailureStateDocument
{
    public required string Dataset { get; init; }

    public required string ItemKey { get; init; }

    public required string LastError { get; set; }

    public int RetryCount { get; set; }

    public DateTimeOffset LastSeenUtc { get; set; }
}

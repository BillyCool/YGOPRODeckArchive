using System.Globalization;
using System.Reflection;
using System.Text;
using System.Text.Json;

namespace YGOPRODeckArchive;

internal static class ArchivePaths
{
    private static readonly char[] InvalidFileNameChars = Path.GetInvalidFileNameChars();

    public static ArchiveLayout Create(string rootDirectory)
    {
        Directory.CreateDirectory(rootDirectory);

        string manifestDirectory = Path.Combine(rootDirectory, "manifest");
        string runHistoryDirectory = Path.Combine(manifestDirectory, "run-history");
        string sourceApiDirectory = Path.Combine(rootDirectory, "source", "api");
        string cardsDirectory = Path.Combine(rootDirectory, "cards");
        string setsDirectory = Path.Combine(rootDirectory, "sets");
        string logsDirectory = Path.Combine(rootDirectory, "logs");

        Directory.CreateDirectory(manifestDirectory);
        Directory.CreateDirectory(runHistoryDirectory);
        Directory.CreateDirectory(sourceApiDirectory);
        Directory.CreateDirectory(cardsDirectory);
        Directory.CreateDirectory(setsDirectory);
        Directory.CreateDirectory(logsDirectory);

        return new ArchiveLayout
        {
            RootDirectory = rootDirectory,
            ManifestDirectory = manifestDirectory,
            RunHistoryDirectory = runHistoryDirectory,
            SourceApiDirectory = sourceApiDirectory,
            CardsDirectory = cardsDirectory,
            SetsDirectory = setsDirectory,
            LogsDirectory = logsDirectory,
            LatestLogPath = Path.Combine(logsDirectory, "latest.log"),
            ArchiveStatePath = Path.Combine(manifestDirectory, "archive-state.json"),
            CardsIndexPath = Path.Combine(manifestDirectory, "cards.index.json"),
            CardLanguagesIndexPath = Path.Combine(manifestDirectory, "card-languages.index.json"),
            SetsIndexPath = Path.Combine(manifestDirectory, "sets.index.json")
        };
    }

    public static string CreateStableSetKey(CardSetListItemDto set, int duplicateOrdinal)
    {
        string setCode = SlugifyKeyPart(set.SetCode, "unknown");
        string date = string.IsNullOrWhiteSpace(set.TcgDate) ? "unknown-date" : set.TcgDate!.Trim();
        string cardCount = set.NumOfCards?.ToString(CultureInfo.InvariantCulture) ?? "unknown-count";
        string baseKey = $"tcg-{setCode}-{date}-{cardCount}";

        return duplicateOrdinal > 1
            ? $"{baseKey}-{duplicateOrdinal:00}"
            : baseKey;
    }

    public static string EnsureEntityDirectory(string parentDirectory, string stablePrefix, string displayName)
    {
        Directory.CreateDirectory(parentDirectory);

        string sanitizedName = SanitizeDisplayName(displayName);
        string expectedDirectory = Path.Combine(parentDirectory, $"{sanitizedName} [{stablePrefix}]");
        List<string> existingDirectories = [.. FindMatchingEntityDirectories(parentDirectory, stablePrefix)];

        if (Directory.Exists(expectedDirectory))
        {
            foreach (string? existingDirectory in existingDirectories.Where(path => !PathEquals(path, expectedDirectory)))
            {
                MergeDirectory(existingDirectory, expectedDirectory);
            }

            return expectedDirectory;
        }

        if (existingDirectories.Count == 1)
        {
            Directory.Move(existingDirectories[0], expectedDirectory);
            return expectedDirectory;
        }

        Directory.CreateDirectory(expectedDirectory);

        foreach (string existingDirectory in existingDirectories)
        {
            MergeDirectory(existingDirectory, expectedDirectory);
        }

        return expectedDirectory;
    }

    public static string? FindEntityDirectory(string parentDirectory, string stablePrefix)
    {
        if (!Directory.Exists(parentDirectory))
        {
            return null;
        }

        return FindMatchingEntityDirectories(parentDirectory, stablePrefix)
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault();
    }

    public static string SanitizeDisplayName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "Unnamed";
        }

        StringBuilder builder = new(value.Length);
        bool lastWasWhitespace = false;

        foreach (char character in value.Trim())
        {
            if (InvalidFileNameChars.Contains(character))
            {
                continue;
            }

            if (char.IsWhiteSpace(character))
            {
                if (!lastWasWhitespace)
                {
                    builder.Append(' ');
                    lastWasWhitespace = true;
                }

                continue;
            }

            builder.Append(character);
            lastWasWhitespace = false;
        }

        string sanitized = builder
            .ToString()
            .Trim()
            .TrimEnd('.', ' ');

        if (sanitized.Length > 120)
        {
            sanitized = sanitized[..120].TrimEnd('.', ' ');
        }

        return string.IsNullOrWhiteSpace(sanitized) ? "Unnamed" : sanitized;
    }

    public static string SlugifyKeyPart(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        StringBuilder builder = new(value.Length);
        bool lastWasDash = false;

        foreach (char character in value.Trim().ToLowerInvariant())
        {
            if (char.IsLetterOrDigit(character))
            {
                builder.Append(character);
                lastWasDash = false;
            }
            else if (!lastWasDash)
            {
                builder.Append('-');
                lastWasDash = true;
            }
        }

        string slug = builder.ToString().Trim('-');
        return string.IsNullOrWhiteSpace(slug) ? fallback : slug;
    }

    public static string GetRelativePath(string rootDirectory, string fullPath)
    {
        return Path.GetRelativePath(rootDirectory, fullPath);
    }

    public static string GetApplicationVersion()
    {
        return Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "1.0.0";
    }

    public static string BuildRawPagePath(string sourceApiDirectory, string dataset, int pageNumber)
    {
        return Path.Combine(sourceApiDirectory, $"{dataset}.page-{pageNumber:00000}.json");
    }

    public static string GetImageFileExtension(string? imageUrl)
    {
        if (Uri.TryCreate(imageUrl, UriKind.Absolute, out Uri? uri))
        {
            string extension = Path.GetExtension(uri.AbsolutePath);

            if (!string.IsNullOrWhiteSpace(extension))
            {
                return extension.ToLowerInvariant();
            }
        }

        return ".jpg";
    }

    private static bool PathEquals(string left, string right)
    {
        return string.Equals(
            Path.GetFullPath(left).TrimEnd(Path.DirectorySeparatorChar),
            Path.GetFullPath(right).TrimEnd(Path.DirectorySeparatorChar),
            StringComparison.OrdinalIgnoreCase);
    }

    private static IEnumerable<string> FindMatchingEntityDirectories(string parentDirectory, string stablePrefix)
    {
        string suffix = $" [{stablePrefix}]";

        return Directory
            .EnumerateDirectories(parentDirectory, "*", SearchOption.TopDirectoryOnly)
            .Where(path => Path.GetFileName(path).EndsWith(suffix, StringComparison.OrdinalIgnoreCase));
    }

    private static void MergeDirectory(string sourceDirectory, string targetDirectory)
    {
        if (PathEquals(sourceDirectory, targetDirectory) || !Directory.Exists(sourceDirectory))
        {
            return;
        }

        Directory.CreateDirectory(targetDirectory);

        foreach (string directory in Directory.EnumerateDirectories(sourceDirectory))
        {
            string childTarget = Path.Combine(targetDirectory, Path.GetFileName(directory));
            MergeDirectory(directory, childTarget);
        }

        foreach (string file in Directory.EnumerateFiles(sourceDirectory))
        {
            string targetFile = Path.Combine(targetDirectory, Path.GetFileName(file));

            if (File.Exists(targetFile))
            {
                FileInfo sourceInfo = new(file);
                FileInfo targetInfo = new(targetFile);

                if (sourceInfo.LastWriteTimeUtc > targetInfo.LastWriteTimeUtc || sourceInfo.Length != targetInfo.Length)
                {
                    File.Copy(file, targetFile, overwrite: true);
                }

                File.Delete(file);
                continue;
            }

            File.Move(file, targetFile);
        }

        if (!Directory.EnumerateFileSystemEntries(sourceDirectory).Any())
        {
            Directory.Delete(sourceDirectory, recursive: false);
        }
    }
}

internal sealed class ArchiveLogger : IAsyncDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly StreamWriter _latestWriter;
    private readonly StreamWriter _historyWriter;
    private readonly ArchiveConsoleProgress? _consoleProgress;

    private ArchiveLogger(StreamWriter latestWriter, StreamWriter historyWriter, ArchiveConsoleProgress? consoleProgress)
    {
        _latestWriter = latestWriter;
        _historyWriter = historyWriter;
        _consoleProgress = consoleProgress;
    }

    public static async Task<ArchiveLogger> CreateAsync(
        ArchiveLayout layout,
        ArchiveConsoleProgress? consoleProgress,
        CancellationToken cancellationToken)
    {
        string runHistoryFileName = $"archive-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}.log";
        string runHistoryPath = Path.Combine(layout.RunHistoryDirectory, runHistoryFileName);

        StreamWriter latestWriter = new(new FileStream(layout.LatestLogPath, FileMode.Create, FileAccess.Write, FileShare.Read))
        {
            AutoFlush = true
        };

        StreamWriter historyWriter = new(new FileStream(runHistoryPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read))
        {
            AutoFlush = true
        };

        ArchiveLogger logger = new(latestWriter, historyWriter, consoleProgress);
        await logger.InfoAsync("Starting archive run.", cancellationToken);
        return logger;
    }

    public Task InfoAsync(string message, CancellationToken cancellationToken = default)
    {
        return WriteAsync("INFO", message, cancellationToken);
    }

    public Task WarnAsync(string message, CancellationToken cancellationToken = default)
    {
        return WriteAsync("WARN", message, cancellationToken);
    }

    public Task ProgressAsync(string message, CancellationToken cancellationToken = default)
    {
        return WriteAsync("PROGRESS", message, cancellationToken);
    }

    public Task ErrorAsync(string message, CancellationToken cancellationToken = default)
    {
        return WriteAsync("ERROR", message, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _historyWriter.DisposeAsync();
        await _latestWriter.DisposeAsync();
        _gate.Dispose();
    }

    private async Task WriteAsync(string level, string message, CancellationToken cancellationToken)
    {
        string line = $"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss}] [{level}] {message}";

        await _gate.WaitAsync(cancellationToken);

        try
        {
            if (_consoleProgress is null)
            {
                Console.WriteLine(line);
            }
            else if (!string.Equals(level, "PROGRESS", StringComparison.OrdinalIgnoreCase))
            {
                _consoleProgress.WriteLogLine(level, line);
            }

            await _latestWriter.WriteLineAsync(line);
            await _historyWriter.WriteLineAsync(line);
        }
        finally
        {
            _gate.Release();
        }
    }
}

internal static class JsonFileStore
{
    private static readonly JsonSerializerOptions ReadOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    private static readonly JsonSerializerOptions WriteOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = true
    };

    public static async Task<T?> ReadJsonAsync<T>(string path, CancellationToken cancellationToken)
    {
        if (!File.Exists(path))
        {
            return default;
        }

        await using FileStream stream = File.OpenRead(path);
        return await JsonSerializer.DeserializeAsync<T>(stream, ReadOptions, cancellationToken);
    }

    public static async Task<bool> WriteJsonIfChangedAsync<T>(string path, T value, CancellationToken cancellationToken)
    {
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(value, WriteOptions);
        return await WriteBytesIfChangedAsync(path, bytes, cancellationToken);
    }

    public static async Task<bool> WriteTextIfChangedAsync(string path, string content, CancellationToken cancellationToken)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(content);
        return await WriteBytesIfChangedAsync(path, bytes, cancellationToken);
    }

    public static async Task<bool> WriteStreamAtomicallyAsync(string path, Stream sourceStream, CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        string temporaryPath = $"{path}.{Guid.NewGuid():N}.tmp";

        await using (FileStream outputStream = new(temporaryPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
        {
            await sourceStream.CopyToAsync(outputStream, cancellationToken);
        }

        if (File.Exists(path))
        {
            File.Move(temporaryPath, path, overwrite: true);
        }
        else
        {
            File.Move(temporaryPath, path);
        }

        return true;
    }

    private static async Task<bool> WriteBytesIfChangedAsync(string path, byte[] newBytes, CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        if (File.Exists(path))
        {
            byte[] existingBytes = await File.ReadAllBytesAsync(path, cancellationToken);

            if (existingBytes.AsSpan().SequenceEqual(newBytes))
            {
                return false;
            }
        }

        string temporaryPath = $"{path}.{Guid.NewGuid():N}.tmp";
        await File.WriteAllBytesAsync(temporaryPath, newBytes, cancellationToken);

        if (File.Exists(path))
        {
            File.Move(temporaryPath, path, overwrite: true);
        }
        else
        {
            File.Move(temporaryPath, path);
        }

        return true;
    }
}

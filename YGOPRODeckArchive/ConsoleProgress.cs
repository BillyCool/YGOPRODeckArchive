using Spectre.Console;
using Spectre.Console.Rendering;

namespace YGOPRODeckArchive;

internal sealed class ArchiveConsoleProgress
{
    private readonly IAnsiConsole _console;
    private readonly ProgressTask _overallTask;
    private readonly object _sync = new();
    private readonly Dictionary<string, int> _datasetTotals = new(StringComparer.OrdinalIgnoreCase);

    private int _completedWorkItems;
    private int _startedDownloadCount;
    private int _completedDownloadCount;
    private int _activeDownloads;
    private int _setTotal;
    private string _activity = "Preparing archive";

    private ArchiveConsoleProgress(IAnsiConsole console, ProgressContext context)
    {
        _console = console;
        _overallTask = context.AddTask("[green]Overall archive[/]", maxValue: 1);
        _overallTask.IsIndeterminate = true;
    }

    public static async Task<T> RunAsync<T>(Func<ArchiveConsoleProgress?, Task<T>> operation)
    {
        if (Console.IsOutputRedirected || !AnsiConsole.Profile.Capabilities.Interactive)
        {
            return await operation(null);
        }

        T? result = default;
        ArchiveConsoleProgress? progress = null;
        Progress display = AnsiConsole.Progress()
            .Columns(
            [
                new SpinnerColumn(),
                new TaskDescriptionColumn(),
                new ProgressBarColumn(),
                new PercentageColumn(),
                new ElapsedTimeColumn()
            ]);

        display.RenderHook = (renderable, _) => progress is null
            ? renderable
            : new Rows(renderable, progress.BuildStatusRenderable());

        await display.StartAsync(async context =>
            {
                progress = new ArchiveConsoleProgress(AnsiConsole.Console, context);

                try
                {
                    result = await operation(progress);
                    progress.Complete();
                }
                catch
                {
                    progress.Complete();
                    throw;
                }
            });

        return result!;
    }

    public void RegisterDatasetTotal(string datasetName, int totalItems)
    {
        if (string.IsNullOrWhiteSpace(datasetName) || totalItems <= 0)
        {
            return;
        }

        lock (_sync)
        {
            _datasetTotals[datasetName] = Math.Max(totalItems, _datasetTotals.GetValueOrDefault(datasetName));
            RefreshOverallTaskLocked();
        }
    }

    public void RegisterSetTotal(int totalSets)
    {
        if (totalSets <= 0)
        {
            return;
        }

        lock (_sync)
        {
            _setTotal = Math.Max(_setTotal, totalSets);
            RefreshOverallTaskLocked();
        }
    }

    public void AdvanceOverall(int completedItems, string? activity = null)
    {
        lock (_sync)
        {
            if (completedItems > 0)
            {
                _completedWorkItems += completedItems;
                RefreshOverallTaskLocked();
            }
        }
    }

    public void SetActivity(string activity)
    {
        lock (_sync)
        {
            SetActivityLocked(activity);
        }
    }

    public void DownloadStarted(string assetLabel)
    {
        _ = assetLabel;

        lock (_sync)
        {
            _startedDownloadCount++;
            _activeDownloads++;
        }
    }

    public void DownloadCompleted(string assetLabel)
    {
        _ = assetLabel;

        lock (_sync)
        {
            _completedDownloadCount++;
            _activeDownloads = Math.Max(0, _activeDownloads - 1);
        }
    }

    public void WriteLogLine(string level, string line)
    {
        string color = level switch
        {
            "ERROR" => "red",
            "WARN" => "yellow",
            "INFO" => "grey",
            _ => "silver"
        };

        lock (_sync)
        {
            _console.MarkupLine($"[{color}]{Markup.Escape(line)}[/]");
        }
    }

    private void Complete()
    {
        lock (_sync)
        {
            RefreshOverallTaskLocked();
            _activeDownloads = 0;
            _activity = "Archive complete";
        }
    }

    private void RefreshOverallTaskLocked()
    {
        int totalWorkItems = _datasetTotals.Values.Sum() + _setTotal;

        if (totalWorkItems <= 0)
        {
            _overallTask.IsIndeterminate = true;
            _overallTask.MaxValue = 1;
            _overallTask.Value = 0;
            _overallTask.Description = "[green]Overall archive[/] estimating work";
            return;
        }

        _overallTask.IsIndeterminate = false;
        _overallTask.MaxValue = totalWorkItems;
        _overallTask.Value = Math.Min(_completedWorkItems, totalWorkItems);
        _overallTask.Description = $"[green]Overall archive[/] {_completedWorkItems:N0}/{totalWorkItems:N0}";
    }

    private string BuildDownloadDescription()
    {
        if (_startedDownloadCount <= 0)
        {
            return string.Empty;
        }

        if (_activeDownloads > 0)
        {
            return $"downloads: {_activeDownloads:N0} active, {_completedDownloadCount:N0} done";
        }

        return $"downloads: {_completedDownloadCount:N0} done";
    }

    private void SetActivityLocked(string activity)
    {
        if (string.IsNullOrWhiteSpace(activity))
        {
            return;
        }

        _activity = activity.Trim();
    }

    private IRenderable BuildStatusRenderable()
    {
        lock (_sync)
        {
            string activity = Normalize(_activity);
            string downloadDescription = BuildDownloadDescription();

            return new Markup(string.IsNullOrEmpty(downloadDescription)
                ? $"[blue]Status[/] {Markup.Escape(activity)}"
                : $"[blue]Status[/] {Markup.Escape(activity)} [grey]|[/] {Markup.Escape(downloadDescription)}");
        }
    }

    private static string Normalize(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "Working";
        }

        return string.Join(' ', value.Split(' ', StringSplitOptions.RemoveEmptyEntries));
    }
}

namespace YGOPRODeckArchive;

public static class Program
{
    private const string ArchiveRootPlaceholder = "<archive-root>";

    public static async Task<int> Main(string[] args)
    {
        CliOptions options;

        try
        {
            options = CliOptions.Parse(args);
        }
        catch (ArgumentException exception)
        {
            Console.Error.WriteLine(exception.Message);
            Console.Error.WriteLine();
            CliOptions.WriteUsage(ArchiveRootPlaceholder);
            return 1;
        }

        if (options.ShowHelp)
        {
            CliOptions.WriteUsage(ArchiveRootPlaceholder);
            return 0;
        }

        using CancellationTokenSource cancellationTokenSource = new();

        void cancelHandler(object? _, ConsoleCancelEventArgs eventArgs)
        {
            eventArgs.Cancel = true;
            cancellationTokenSource.Cancel();
            Console.WriteLine("Cancellation requested. Finishing the current operation before stopping...");
        }

        Console.CancelKeyPress += cancelHandler;

        try
        {
            ArchiveRunner runner = new();
            return await runner.RunAsync(options, cancellationTokenSource.Token);
        }
        finally
        {
            Console.CancelKeyPress -= cancelHandler;
        }
    }
}

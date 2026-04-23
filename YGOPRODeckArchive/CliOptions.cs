using System.Globalization;

namespace YGOPRODeckArchive;

internal sealed class CliOptions
{
    public required string RootPath { get; init; }

    public bool ForceRefresh { get; init; }

    public bool KeepRawPages { get; init; }

    public int PageSize { get; init; }

    public int? MaxPages { get; init; }

    public bool ShowHelp { get; init; }

    public static CliOptions Parse(IReadOnlyList<string> args)
    {
        string? rootPath = null;
        bool forceRefresh = false;
        bool keepRawPages = false;
        int pageSize = 100;
        int? maxPages = null;
        bool showHelp = false;

        for (int index = 0; index < args.Count; index++)
        {
            string argument = args[index];

            switch (argument)
            {
                case "archive":
                    break;

                case "-h":
                case "--help":
                    showHelp = true;
                    break;

                case "--root":
                    rootPath = ReadValue(args, ref index, argument);
                    break;

                case "--force-refresh":
                    forceRefresh = true;
                    break;

                case "--keep-raw-pages":
                    keepRawPages = true;
                    break;

                case "--page-size":
                    pageSize = ParsePositiveInt(ReadValue(args, ref index, argument), argument);
                    break;

                case "--max-pages":
                    maxPages = ParsePositiveInt(ReadValue(args, ref index, argument), argument);
                    break;

                default:
                    throw new ArgumentException($"Unknown argument '{argument}'.");
            }
        }

        if (!showHelp && string.IsNullOrWhiteSpace(rootPath))
        {
            throw new ArgumentException("Missing required '--root <path>' argument.");
        }

        return new CliOptions
        {
            RootPath = string.IsNullOrWhiteSpace(rootPath) ? string.Empty : Path.GetFullPath(rootPath),
            ForceRefresh = forceRefresh,
            KeepRawPages = keepRawPages,
            PageSize = pageSize,
            MaxPages = maxPages,
            ShowHelp = showHelp
        };
    }

    public static void WriteUsage(string rootPlaceholder)
    {
        Console.WriteLine("YGOPRODeckArchive");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine($"  YGOPRODeckArchive [archive] --root {rootPlaceholder} [--force-refresh] [--keep-raw-pages]");
        Console.WriteLine("                    [--page-size <n>] [--max-pages <n>]");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --root <path>        Archive destination. Required.");
        Console.WriteLine("  --force-refresh      Re-download images and rewrite raw API pages.");
        Console.WriteLine("  --keep-raw-pages     Save raw API payloads under source\\api.");
        Console.WriteLine("  --page-size <n>      Number of cards requested per API page. Default: 100");
        Console.WriteLine("  --max-pages <n>      Optional page limit per dataset, useful for smoke tests.");
        Console.WriteLine("  -h, --help           Show this help.");
    }

    private static string ReadValue(IReadOnlyList<string> args, ref int index, string argumentName)
    {
        if (index + 1 >= args.Count)
        {
            throw new ArgumentException($"Missing value for '{argumentName}'.");
        }

        index++;
        return args[index];
    }

    private static int ParsePositiveInt(string value, string argumentName)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsedValue) || parsedValue <= 0)
        {
            throw new ArgumentException($"'{argumentName}' expects a positive integer.");
        }

        return parsedValue;
    }
}

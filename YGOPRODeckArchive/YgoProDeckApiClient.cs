using System.Net;
using System.Text.Json;

namespace YGOPRODeckArchive;

internal sealed class YgoProDeckApiClient : IAsyncDisposable
{
    private static readonly Uri BaseUri = new("https://db.ygoprodeck.com/api/v7/");
    private static readonly TimeSpan MinimumRequestGap = TimeSpan.FromMilliseconds(60);
    private static readonly JsonSerializerOptions DeserializerOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _httpClient;
    private readonly SemaphoreSlim _requestGate = new(1, 1);
    private DateTimeOffset _lastRequestUtc = DateTimeOffset.MinValue;

    public YgoProDeckApiClient()
    {
        _httpClient = new HttpClient
        {
            BaseAddress = BaseUri,
            Timeout = TimeSpan.FromMinutes(2)
        };
    }

    public Task<ApiResponse<CardInfoPageDto>> GetCardPageAsync(string language, int pageSize, int offset, CancellationToken cancellationToken)
    {
        string relativeUrl = string.Equals(language, "en", StringComparison.OrdinalIgnoreCase)
            ? $"cardinfo.php?num={pageSize}&offset={offset}"
            : $"cardinfo.php?num={pageSize}&offset={offset}&language={Uri.EscapeDataString(language)}";

        return GetJsonAsync<CardInfoPageDto>(relativeUrl, cancellationToken);
    }

    public Task<ApiResponse<List<CardSetListItemDto>>> GetSetListAsync(CancellationToken cancellationToken)
    {
        return GetJsonAsync<List<CardSetListItemDto>>("cardsets.php", cancellationToken);
    }

    public async Task DownloadFileAsync(string absoluteUrl, string destinationPath, CancellationToken cancellationToken)
    {
        const int maxAttempts = 3;
        Exception? lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                using HttpResponseMessage response = await SendAsync(
                    () => new HttpRequestMessage(HttpMethod.Get, absoluteUrl),
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken);

                response.EnsureSuccessStatusCode();

                await using Stream responseStream = await response.Content.ReadAsStreamAsync(cancellationToken);
                await JsonFileStore.WriteStreamAtomicallyAsync(destinationPath, responseStream, cancellationToken);
                return;
            }
            catch (Exception exception) when (attempt < maxAttempts && IsTransient(exception))
            {
                lastException = exception;
                await Task.Delay(TimeSpan.FromSeconds(attempt), cancellationToken);
            }
        }

        throw lastException ?? new InvalidOperationException($"Failed to download '{absoluteUrl}'.");
    }

    public async ValueTask DisposeAsync()
    {
        _requestGate.Dispose();
        _httpClient.Dispose();
        await ValueTask.CompletedTask;
    }

    private async Task<ApiResponse<T>> GetJsonAsync<T>(string relativeUrl, CancellationToken cancellationToken)
    {
        const int maxAttempts = 3;
        Exception? lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                using HttpResponseMessage response = await SendAsync(
                    () => new HttpRequestMessage(HttpMethod.Get, relativeUrl),
                    HttpCompletionOption.ResponseContentRead,
                    cancellationToken);

                response.EnsureSuccessStatusCode();
                string rawJson = await response.Content.ReadAsStringAsync(cancellationToken);
                T value = JsonSerializer.Deserialize<T>(rawJson, DeserializerOptions)
                    ?? throw new InvalidOperationException($"Received an empty JSON body for '{relativeUrl}'.");

                return new ApiResponse<T>(rawJson, value);
            }
            catch (Exception exception) when (attempt < maxAttempts && IsTransient(exception))
            {
                lastException = exception;
                await Task.Delay(TimeSpan.FromSeconds(attempt), cancellationToken);
            }
        }

        throw lastException ?? new InvalidOperationException($"Failed to fetch '{relativeUrl}'.");
    }

    private async Task<HttpResponseMessage> SendAsync(
        Func<HttpRequestMessage> requestFactory,
        HttpCompletionOption completionOption,
        CancellationToken cancellationToken)
    {
        await _requestGate.WaitAsync(cancellationToken);

        try
        {
            TimeSpan delay = MinimumRequestGap - (DateTimeOffset.UtcNow - _lastRequestUtc);

            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, cancellationToken);
            }

            _lastRequestUtc = DateTimeOffset.UtcNow;
        }
        finally
        {
            _requestGate.Release();
        }

        return await _httpClient.SendAsync(requestFactory(), completionOption, cancellationToken);
    }

    private static bool IsTransient(Exception exception)
    {
        return exception switch
        {
            HttpRequestException httpRequestException when httpRequestException.StatusCode is null => true,
            HttpRequestException httpRequestException when httpRequestException.StatusCode is HttpStatusCode.TooManyRequests => true,
            HttpRequestException httpRequestException when httpRequestException.StatusCode is HttpStatusCode.BadGateway or HttpStatusCode.ServiceUnavailable or HttpStatusCode.GatewayTimeout => true,
            TaskCanceledException => true,
            _ => false
        };
    }
}

internal sealed record ApiResponse<T>(string RawJson, T Value);

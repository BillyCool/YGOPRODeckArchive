# YGOPRODeckArchive

A .NET 10 console app that builds a local archive of Yu-Gi-Oh! data from the public [YGOPRODeck API](https://ygoprodeck.com/api-guide/).

It is designed to:

- archive card data
- archive set data
- download the highest-quality card and set images exposed by the API
- resume after interruption
- skip work that is already archived
- refresh mutable data such as banlist status on later runs

## What it archives

### Cards

For each card, the app stores:

- canonical English card data
- translated card text for the public API languages: `fr`, `de`, `it`, `pt`
- card metadata such as type, stats, archetype, identifiers, set membership, and banlist info
- full card image, small card image, and cropped artwork image

### Sets

For each set, the app stores:

- set metadata from `cardsets.php`
- set image when the API provides one
- a `cards.json` file containing the card IDs that belong to that set

### Not archived

The app intentionally does **not** store market/price data:

- `card_prices`
- `set_price`

It also stores the **current** banlist state only, not historical banlist snapshots.

## Requirements

- .NET 10 SDK
- Internet access to the YGOPRODeck API and image host
- write access to the archive destination

## Usage

From the repository root:

```powershell
dotnet run --project .\YGOPRODeckArchive\YGOPRODeckArchive.csproj -- --root D:\Archives\Yu-Gi-Oh
```

Show help:

```powershell
dotnet run --project .\YGOPRODeckArchive\YGOPRODeckArchive.csproj -- --help
```

Archive to a custom location:

```powershell
dotnet run --project .\YGOPRODeckArchive\YGOPRODeckArchive.csproj -- --root D:\Archives\Yu-Gi-Oh
```

Force image/raw-page refresh:

```powershell
dotnet run --project .\YGOPRODeckArchive\YGOPRODeckArchive.csproj -- --root D:\Archives\Yu-Gi-Oh --force-refresh
```

Keep raw API payloads under `source\api`:

```powershell
dotnet run --project .\YGOPRODeckArchive\YGOPRODeckArchive.csproj -- --root D:\Archives\Yu-Gi-Oh --keep-raw-pages
```

Run a limited smoke test:

```powershell
dotnet run --project .\YGOPRODeckArchive\YGOPRODeckArchive.csproj -- --root D:\Archives\Yu-Gi-Oh --page-size 2 --max-pages 1 --keep-raw-pages
```

The optional `archive` positional token is also accepted:

```powershell
dotnet run --project .\YGOPRODeckArchive\YGOPRODeckArchive.csproj -- archive --root D:\Archives\Yu-Gi-Oh
```

## Command-line options

| Option | Description |
| --- | --- |
| `--root <path>` | Archive destination. Required |
| `--force-refresh` | Re-download images and rewrite raw API pages |
| `--keep-raw-pages` | Save raw API payloads under `source\api` |
| `--page-size <n>` | Number of cards requested per API page. Default: `100` |
| `--max-pages <n>` | Optional page limit per dataset, mainly useful for smoke tests |
| `-h`, `--help` | Show help |

## Archive layout

```text
<archive-root>\
  cards\
    <card-name> [<card-id>]\
      card.en.json
      card.fr.json
      card.de.json
      card.it.json
      card.pt.json
      images\
        full.jpg
        small.jpg
        art-cropped.jpg
  sets\
    <set-name> [<set-key>]\
      set.json
      cards.json
      image.jpg
  manifest\
    archive-state.json
    cards.index.json
    card-languages.index.json
    sets.index.json
    run-history\
  logs\
    latest.log
  source\
    api\
```

## Resume and update behavior

- Re-running the app against the same archive root performs an incremental sync.
- Existing JSON files are only rewritten when content changes.
- Existing images are skipped unless `--force-refresh` is used.
- If a card or set name changes upstream, the folder name is updated while keeping the same stable ID/key prefix.
- Pressing `Ctrl+C` requests a graceful stop. The app writes progress as it goes, so a later run can continue.
- The app writes runtime progress to the console and mirrors it to `logs\latest.log` and `manifest\run-history\`.

## Notes

- Set metadata comes from `cardsets.php`.
- Set membership is derived from the archived English card data.
- Translation support is limited to the public API languages currently handled by the app: `en`, `fr`, `de`, `it`, `pt`.
- Folder names are sanitized to be Windows-compatible.

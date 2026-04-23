using System.Text.Json.Serialization;

namespace YGOPRODeckArchive;

internal sealed class CardInfoPageDto
{
    [JsonPropertyName("data")]
    public List<CardDto> Data { get; init; } = [];

    [JsonPropertyName("meta")]
    public CardInfoMetaDto? Meta { get; init; }
}

internal sealed class CardInfoMetaDto
{
    [JsonPropertyName("generated")]
    public string? Generated { get; init; }

    [JsonPropertyName("current_rows")]
    public int? CurrentRows { get; init; }

    [JsonPropertyName("total_rows")]
    public int? TotalRows { get; init; }

    [JsonPropertyName("rows_remaining")]
    public int? RowsRemaining { get; init; }

    [JsonPropertyName("total_pages")]
    public int? TotalPages { get; init; }

    [JsonPropertyName("pages_remaining")]
    public int? PagesRemaining { get; init; }

    [JsonPropertyName("next_page")]
    public string? NextPage { get; init; }

    [JsonPropertyName("next_page_offset")]
    public int? NextPageOffset { get; init; }
}

internal sealed class CardDto
{
    [JsonPropertyName("id")]
    public int Id { get; init; }

    [JsonPropertyName("konami_id")]
    public int? KonamiId { get; init; }

    [JsonPropertyName("name")]
    public string Name { get; init; } = string.Empty;

    [JsonPropertyName("name_en")]
    public string? NameEn { get; init; }

    [JsonPropertyName("type")]
    public string? Type { get; init; }

    [JsonPropertyName("humanReadableCardType")]
    public string? HumanReadableCardType { get; init; }

    [JsonPropertyName("frameType")]
    public string? FrameType { get; init; }

    [JsonPropertyName("typeline")]
    public List<string>? Typeline { get; init; }

    [JsonPropertyName("desc")]
    public string? Desc { get; init; }

    [JsonPropertyName("pend_desc")]
    public string? PendDesc { get; init; }

    [JsonPropertyName("monster_desc")]
    public string? MonsterDesc { get; init; }

    [JsonPropertyName("atk")]
    public int? Atk { get; init; }

    [JsonPropertyName("def")]
    public int? Def { get; init; }

    [JsonPropertyName("level")]
    public int? Level { get; init; }

    [JsonPropertyName("rank")]
    public int? Rank { get; init; }

    [JsonPropertyName("scale")]
    public int? Scale { get; init; }

    [JsonPropertyName("linkval")]
    public int? LinkVal { get; init; }

    [JsonPropertyName("linkmarkers")]
    public List<string>? LinkMarkers { get; init; }

    [JsonPropertyName("race")]
    public string? Race { get; init; }

    [JsonPropertyName("attribute")]
    public string? Attribute { get; init; }

    [JsonPropertyName("archetype")]
    public string? Archetype { get; init; }

    [JsonPropertyName("ygoprodeck_url")]
    public string? YgoprodeckUrl { get; init; }

    [JsonPropertyName("banlist_info")]
    public BanlistInfoDto? BanlistInfo { get; init; }

    [JsonPropertyName("card_sets")]
    public List<CardSetPrintDto>? CardSets { get; init; }

    [JsonPropertyName("card_images")]
    public List<CardImageDto>? CardImages { get; init; }
}

internal sealed class BanlistInfoDto
{
    [JsonPropertyName("ban_tcg")]
    public string? BanTcg { get; init; }

    [JsonPropertyName("ban_ocg")]
    public string? BanOcg { get; init; }

    [JsonPropertyName("ban_goat")]
    public string? BanGoat { get; init; }

    [JsonPropertyName("ban_edison")]
    public string? BanEdison { get; init; }
}

internal sealed class CardSetPrintDto
{
    [JsonPropertyName("set_name")]
    public string SetName { get; init; } = string.Empty;

    [JsonPropertyName("set_code")]
    public string? SetCode { get; init; }

    [JsonPropertyName("set_rarity")]
    public string? SetRarity { get; init; }

    [JsonPropertyName("set_rarity_code")]
    public string? SetRarityCode { get; init; }

    [JsonPropertyName("set_price")]
    public string? SetPrice { get; init; }
}

internal sealed class CardImageDto
{
    [JsonPropertyName("id")]
    public int? Id { get; init; }

    [JsonPropertyName("image_url")]
    public string? ImageUrl { get; init; }

    [JsonPropertyName("image_url_small")]
    public string? ImageUrlSmall { get; init; }

    [JsonPropertyName("image_url_cropped")]
    public string? ImageUrlCropped { get; init; }
}

internal sealed class CardSetListItemDto
{
    [JsonPropertyName("set_name")]
    public string SetName { get; init; } = string.Empty;

    [JsonPropertyName("set_code")]
    public string? SetCode { get; init; }

    [JsonPropertyName("num_of_cards")]
    public int? NumOfCards { get; init; }

    [JsonPropertyName("tcg_date")]
    public string? TcgDate { get; init; }

    [JsonPropertyName("set_image")]
    public string? SetImage { get; init; }
}

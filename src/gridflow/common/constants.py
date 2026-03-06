COUNTRIES = {
    "GB": "Great Britain",
    "FR": "France",
    "IE": "Ireland",
    "DK": "Denmark",
    "NO": "Norway",
    "BE": "Belgium",
    "NL": "Netherlands",
}

COUNTRY_ALIASES = {
    "UK": "GB",
    "United Kingdom": "GB",
    "Great Britain": "GB",
    "France": "FR",
    "Ireland": "IE",
    "Denmark": "DK",
    "Norway": "NO",
    "Belgium": "BE",
    "Netherlands": "NL",
}

INTERCONNECTORS = {
    "IFA": ("GB", "FR"),
    "IFA2": ("GB", "FR"),
    "BritNed": ("GB", "NL"),
    "Nemo": ("GB", "BE"),
    "NorthSeaLink": ("GB", "NO"),
    "Viking": ("GB", "DK"),
    "Moyle": ("GB", "IE"),
    "EWIC": ("GB", "IE"),
    "Greenlink": ("GB", "IE"),
}

STANDARD_COLUMNS = {
    "timestamp": "timestamp_utc",
    "country": "country_code",
    "from_country": "from_country_code",
    "to_country": "to_country_code",
    "interconnector": "interconnector_name",
    "source": "source",
    "frequency": "frequency",
    "value": "value",
}
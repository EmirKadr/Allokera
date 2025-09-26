# io/schemas.py
from typing import Dict, List

ORDER_SCHEMA: Dict[str, List[str]] = {
    "artikel": ["artikel", "artikelnummer", "sku", "article", "artnr", "art.nr"],
    "qty":     ["beställt", "antal", "qty", "quantity", "bestalld", "order qty"],
    "status":  ["status", "radstatus", "orderstatus", "state"],
    "ordid":   ["ordernr", "order nr", "order number", "kund", "kundnr"],
    "radid":   ["radnr", "rad nr", "line id", "rad", "struktur", "radsnr"],
}

BUFFER_SCHEMA: Dict[str, List[str]] = {
    "artikel": ["artikel", "article", "artnr", "art.nr", "artikelnummer"],
    "qty":     ["antal", "qty", "quantity", "pallantal", "colli", "units"],
    "loc":     ["lagerplats", "plats", "location", "bin", "hyllplats"],
    "dt":      ["datum/tid", "datum", "mottagen", "received", "inleverans", "inleveransdatum", "timestamp", "arrival"],
    "id":      ["pallid", "pall id", "id", "sscc", "etikett", "batch", "lpn"],
    "status":  ["status", "pallstatus", "state"],
}

NOT_PUTAWAY_SCHEMA: Dict[str, List[str]] = {
    "artikel":  ["artikel", "artnr", "art.nr", "artikelnummer"],
    "namn":     ["artikelnamn", "artikelbenämning", "benämning", "produktnamn", "namn", "artikel.1"],
    "antal":    ["antal", "qty", "quantity", "kolli"],
    "status":   ["status"],
    "pallnr":   ["pall nr", "pallid", "pall id", "pall"],
    "sscc":     ["sscc"],
    "andrad":   ["ändrad", "senast ändrad", "timestamp"],
    "utgang":   ["utgång", "bäst före", "utgångsdatum", "utgangsdatum", "best före"],
}

SALDO_SCHEMA: Dict[str, List[str]] = {
    "artikel":    ["artikel", "artnr", "art.nr", "artikelnummer", "sku", "article"],
    "plocksaldo": ["plocksaldo", "plock saldo", "plock-saldo", "saldo", "pick saldo", "pick qty",
                   "tillgängligt plock", "tillgangligt plock", "available pick", "plock"],
    "plockplats": ["plockplats", "huvudplock", "mainpick", "hyllplats", "bin", "location", "lagerplats"],
}

# Sales-plocklogg
PICK_LOG_SCHEMA: dict[str, list[str]] = {
    "artikel": ["artikel", "artikelnr", "artnr", "art.nr", "artikelnummer", "sku", "article"],
    "antal":   ["plockat", "antal", "quantity", "qty", "picked", "units"],
    "datum":   ["datum", "datumtid", "timestamp", "date", "tid", "time"]
}

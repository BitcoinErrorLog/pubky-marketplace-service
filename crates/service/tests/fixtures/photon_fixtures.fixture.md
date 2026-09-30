# Photon search responses

The `photon_*.json` files are unmodified responses from the public Photon
instance (`https://photon.komoot.io/api/`), captured on 2026-09-29 with one
request each, spaced two seconds apart:

| File | Query |
| --- | --- |
| `photon_us_union.json` | `q=42 Union Street New Bedford&limit=5&lang=en&countrycode=US&layer=house&layer=street` |
| `photon_us_house.json` | `q=1600 Pennsylvania Ave Washington&limit=5&lang=en&countrycode=US&layer=house&layer=street` |
| `photon_de_house.json` | `q=Unter den Linden 1 Berlin&limit=5&lang=en&layer=house&layer=street` |
| `photon_poi.json` | `q=Blue Bottle Coffee Oakland&limit=5&lang=en` |

Photon returns `state` as a full name (no state code) and has no address point
for `42 Union Street, New Bedford` (only the street), which is why the
normalizer keeps a typed leading house number for street-level matches.

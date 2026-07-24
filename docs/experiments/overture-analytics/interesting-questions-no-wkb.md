# Overture Maps — Interesting Analytical Questions (no-WKB constraint)

**Date:** 2026-07-22
**Dataset:** Overture Maps, release `2026-07-22.0` (public S3 `s3://overturemaps-us-west-2`)
**Local copy:** `/home/kuhu/Reps/spark-rapids/overture_2026-07-22/` (~125 GiB valid Parquet, native zstd, no transcoding)
**Constraint:** the `geometry` (WKB binary) column is **not** decoded/used as-is.

---

## 1. What the data is (verified from downloaded Parquet)

Four themes, ~1.32 billion rows total. Row counts and schemas read directly from the file footers.

| theme | types | files | rows | brings |
|---|---|---|---|---|
| places | place | 16 | 74,223,561 | POIs: `categories`, `brand`, `confidence`, `websites`/`phones`/`emails`/`socials`, embedded `addresses`, `operating_status`, `names` |
| divisions | division, division_area, division_boundary | 10 | 5,815,633 | admin areas: `country`, `region`, `subtype`, `admin_level`, `population`, `hierarchies`, `capital_*`, `norms.driving_side` |
| addresses | address | 32 | 472,703,893 | address points: `street`, `number`, `unit`, `postcode`, `postal_city`, `country` |
| transportation | connector, segment | 160 | 765,441,282 | road network. **segment** (348.7M): `class`/`subclass`, `names`, `connectors`, `speed_limits`, `access_restrictions`, `road_surface`, `road_flags`, `rail_flags`, `width_rules`, `routes`, `prohibited_transitions`. **connector** (416.8M): nodes — `id`/`geometry`/`bbox` only. |

### Datatypes
Only **5 scalar leaf types** across the whole dataset — `string`, `double`, `int32`, `bool`, `binary` — wrapped in three containers: `struct`, `list`, `map`. No native `timestamp`/`date` (`update_time` is a `string`), no `decimal`, no `int64`, no native geometry type. `geometry` is `binary` (WKB); `bbox` is `struct<xmin,xmax,ymin,ymax: double>`.

---

## 2. Why "no WKB", and what we use instead

RAPIDS Accelerator support, verified in source:

- Binary **reads** on GPU: `ParquetFormatType` `cudfRead` TypeSig includes `TypeSig.BINARY` nested — `GpuOverrides.scala:896-899`. The geometry column (and all struct/list/map nesting) loads on GPU.
- Binary **casts** on GPU: `binaryChecks = STRING + BINARY` (`TypeChecks.scala:1335`); `castBinToString` (`GpuCast.scala:1445-1468`), string/int→binary (`GpuCast.scala:575-576`). But casting WKB to string yields raw/hex **bytes, not coordinates**.
- **No geospatial/WKB decoding anywhere in the plugin** — `grep -rni 'WKB|st_geom|st_contains|sedona|geospatial|geometryUDT' sql-plugin/src/main/scala/` returns nothing. Any true spatial op (length, distance, point-in-polygon) would require a UDF/library running **on the CPU**, off the GPU.

**Conclusion:** don't decode `geometry`; prune it from projections (it is the largest column — column pruning speeds the scan). All spatial reasoning uses one of three GPU-native mechanisms:

- **[join]** — attribute joins on shared text keys: `country`, `region`, `postcode`.
- **[grid]** — bin each row's `bbox` center `((xmin+xmax)/2, (ymin+ymax)/2)` to a lat/lon cell; count/join by cell. Exact counts, coarse geography.
- **[graph]** — the segment `connectors` list (`connector_id`) gives road-network topology (degree, adjacency, turn rules) with no geometry.

---

## 3. The questions

### A. Attribute-join — exact, fully GPU
1. Restaurants/clinics/schools **per capita** by region — `places`→`divisions` on `region`/`country`, normalize by `population`. [join]
2. Brand penetration by country — `places.brand.names.primary` × `country`; who saturates a nation, who stops at the border. [join]
3. Commercial mix per region — category composition of POIs within each division. [join]
4. Road-class composition by country — segment share by `class` per `country`, per capita (count-based, no length). [join]
5. Bridges/tunnels & surface inequality by region — `road_flags` / `road_surface` value shares per country. [join] *(coverage caveat)*
6. Most common street name per country — `addresses.street` frequency by `country`. [join]
7. Addressing completeness by governance — `addresses` count per `population` per region. [join]
8. Open-vs-closed economic vitality — `operating_status` shares of POIs per region. [join]
9. How online is commerce, by country — fraction of POIs with website/phone/email/social, per `country`. [join]
10. Multilingual naming geography — coverage of `names.common` languages by division. [join]

### B. Grid-binned — exact counts per cell, approximate location
11. Business districts vs. bedroom suburbs — POI-density grid vs. address-density grid; classify each cell commercial/residential/mixed. [grid]
12. Commercial intensity index — POIs per 1,000 addresses per cell. [grid]
13. Infrastructure vs. activity — segment-count grid vs. POI-count grid: over-/under-served corridors. [grid]
14. Where the map is blind — cells with addresses but ~no places, or roads but no addresses → under-mapped populations. [grid]+[join]
15. Approximate "nearest service" — for each address cell, does a POI category exist in the same or an adjacent cell? Coarse access/desert map. [grid] *(approximate — cell-granular, state the cell size)*
16. Chains vs. independents, spatially — brand-present vs. brand-absent POI density per cell. [grid]

### C. Graph-topology — exact, needs no geometry
17. Sprawl vs. grid — intersection **degree** distribution (count segments per `connector_id`); grid = many degree-4 nodes, sprawl = many degree-1/3. [graph]
18. What anchors high-degree intersections — rank connectors by degree, then (via bbox cell) which POI categories co-occur. [graph]+[grid]
19. Turn-restriction complexity — density of `prohibited_transitions` per region. [graph]+[join]
20. Named route networks — connected highway/cycle systems via `routes` (`network`,`ref`,`wikidata`). [graph/attr]+[join]
21. Multimodality — footway/cycleway/rail (`class`/`subtype`/`rail_flags`) vs. car roads per capita. [join]
22. Access-restriction landscape — where `access_restrictions` cluster (gated/private/time-limited), by region. [join] *(coverage caveat)*

### D. bbox-extent — approximate size, no decode
23. Sprawl of features — segment `bbox` diagonal as a rough length proxy; long vs. short segments per region. [grid/attr] *(approximate — degrees, ignores curvature)*
24. Division footprint vs. population — division `bbox` extent vs. `population`. [join] *(approximate area)*

### E. Flagship multi-way (reframed for no-WKB)
25. Coarse 15-minute city — per address cell, count distinct daily-need categories in the cell + 8 neighbors; aggregate per division, per capita. [grid]+[join] *(approximate — cell-based, not routed)*
26. Equity of access — the cell-based access map (Q15) aggregated per capita per region. [grid]+[join]
27. Infrastructure–activity–population triangle — per region: road-count density × POI density × `population` → over-built / under-served / balanced / emptying. [join]+[grid]
28. Formal vs. informal urbanization index — addressing regularity (`number`/`street` fill + pattern) + POI `confidence` + connector-degree structure, per division. [join]+[graph]
29. Who mapped the world — `sources` composition across all four themes per region, vs. `confidence` and coverage: provenance bias in the basemap. [join]+[grid]

---

## 4. Caveats

- **Approximate by construction:** anything spatial is now cell-resolution (grid) or bbox-extent — no true distance/length/containment. Flagged questions: 15, 23, 24, 25.
- **Graph questions (17–20) are exact** — `connectors` is topological, not geometric — so "no WKB" costs nothing there.
- **Fill-rate unverified:** Q5, Q22 (and speed limits) lean on sparse, OSM-derived list columns — likely many nulls, uneven by region. Measure coverage before ranking, or "unmapped" reads as "absent."
- **Provenance of numbers:** row counts, schemas, datatypes, and the RAPIDS support citations above are verified from the files / source. Fill-rates and any per-region magnitudes are **not** yet measured.

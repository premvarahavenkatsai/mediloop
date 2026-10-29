# MedLoop Index Benchmark Results

Generated automatically by `python -m scripts.benchmark` using MongoDB `explain("executionStats")`.

| Query Pattern | Index Configuration | Winning Stage | `totalDocsExamined` | `totalKeysExamined` | `nReturned` | `executionTimeMillis` |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: |
| a) `devices.find({category, status})` | Without Index | `COLLSCAN` | 2000 | 0 | 214 | 7 |
| a) `devices.find({category, status})` | With Index `{category: 1, status: 1}` | `IXSCAN` | 214 | 214 | 214 | 12 |
| b) `events.find({device_id}).sort({ts: -1})` | Without Index | `COLLSCAN + SORT` | 57131 | 0 | 28 | 33 |
| b) `events.find({device_id}).sort({ts: -1})` | With Index `{device_id: 1, ts: -1}` | `IXSCAN` | 28 | 28 | 28 | 6 |
| c) `devices` keyword search (`Concentrator`) | Without Text Index (`$regex` scan) | `COLLSCAN` | 2000 | 0 | 218 | 9 |
| c) `devices` keyword search (`Concentrator`) | With Text Index (`$text`) | `TEXT_MATCH (IXSCAN)` | 218 | 218 | 218 | 17 |
| d) `devices.find({location: {$geoWithin: {$centerSphere: ...}}})` | Without `2dsphere` Index | `COLLSCAN` | 2000 | 0 | 668 | 3 |
| d) `devices.find({location: {$geoWithin: {$centerSphere: ...}}})` | With `2dsphere` Index | `IXSCAN` | 668 | 670 | 668 | 10 |
| e) Covered Query: `devices.find({category, status}, {category: 1, status: 1, _id: 0})` | Covered Index Scan (`_id: 0`) | `PROJECTION_COVERED (IXSCAN)` | 0 | 214 | 214 | 1 |

## Key Observations for Viva Defense

1. **Compound Index (`{category: 1, status: 1}`)**: Eliminates a full `COLLSCAN` across all 2,000 devices and inspects only matching index keys.
2. **ESR Compound Index (`{device_id: 1, ts: -1}`)**: Without the index, MongoDB scans all 50,000+ events and performs an in-memory blocking `SORT` stage. With `{device_id: 1, ts: -1}`, events for the target device are read in reverse-chronological order directly from the B-tree.
3. **Geospatial `$geoWithin` + `$centerSphere`**: Unlike `$geoNear` (which refuses to run without a geospatial index), `$geoWithin` runs a full `COLLSCAN` when unindexed and switches to `IXSCAN` over S2 cell tokens once the `2dsphere` index is built.
4. **Covered Query (`_id: 0`)**: Projecting only `{category: 1, status: 1, _id: 0}` allows the WiredTiger storage engine to answer the query entirely from the index leaves without fetching collection documents (`totalDocsExamined = 0`).

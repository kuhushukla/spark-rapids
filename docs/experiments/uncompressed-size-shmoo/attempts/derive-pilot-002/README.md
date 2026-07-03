# Derivation pilot 002

Status: recovered harness attempt; data write valid, initial manifest generation failed.

The same single month completed its Parquet write with a 32 GiB driver heap, producing
28 files. The script then used the wrong census key (`rows` rather than
`row_count`) and raised `KeyError` while writing its manifest.

The completed directory contained `_SUCCESS`; the corrected script re-read and hashed
those files without rewriting them. The resulting pilot manifest verified 28 files and
the expected source identity. This attempt supplied only corpus-preparation evidence,
not performance evidence.

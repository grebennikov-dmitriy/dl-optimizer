def recommend_table_properties():
    return {
        "format-version": "2",
        "write.format.default": "PARQUET",
        "commit.manifest.min-count-to-merge": "5",
        "write.parquet.compression-codec": "zstd",
        "write.parquet.compression-level": "7",
        "write.target-file-size-bytes": str(512 * 1024 * 1024),
        "history.expire.min-snapshots-to-keep": "5",
    }

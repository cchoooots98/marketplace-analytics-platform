-- Singular test: re-running an unchanged seller snapshot source must not
-- generate an extra history version with identical tracked attributes.
{{ snapshot_noop_version_rows(
    'snap_sellers',
    'seller_id',
    [
        'seller_zip_code_prefix',
        'seller_city',
        'seller_state'
    ]
) }}

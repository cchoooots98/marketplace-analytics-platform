-- Singular test: re-running an unchanged product snapshot source must not
-- generate an extra history version with identical tracked attributes.
{{ snapshot_noop_version_rows(
    'snap_products',
    'product_id',
    [
        'product_category_name',
        'product_name_length',
        'product_description_length',
        'product_photos_count',
        'product_weight_g',
        'product_length_cm',
        'product_height_cm',
        'product_width_cm'
    ]
) }}

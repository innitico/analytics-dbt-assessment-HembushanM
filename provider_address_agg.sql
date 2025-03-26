--

WITH provider_addresses AS (
    SELECT
        p.id AS provider_id,
        COALESCE(
            JSON_ARRAYAGG(
                JSON_OBJECT(
                    'address_id', a.id,
                    'street', a.street,
                    'rank', a.rank
                ) ORDER BY a.id
            ) FILTER (WHERE a.id IS NOT NULL),
            '[]'
        )::ARRAY AS addresses
    FROM {{ ref('provider') }} p
    LEFT JOIN {{ ref('provider_address') }} pa ON p.id = pa.provider_id
    LEFT JOIN {{ ref('address') }} a ON pa.address_id = a.id
    GROUP BY p.id
)
SELECT * FROM provider_addresses
--
WITH degrees_parsed AS (
    SELECT
        p.id AS provider_id,
        d.ptui,
        d.rank,
        ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY d.rank DESC) AS rn
    FROM {{ ref('provider') }} p
    CROSS JOIN LATERAL FLATTEN(input => SPLIT(p.degrees, ',')) AS deg
    JOIN {{ ref('degree') }} d ON d.degree = deg.value
)

SELECT provider_id, ptui
FROM degrees_parsed
WHERE rn = 1;

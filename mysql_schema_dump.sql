SELECT TABLE_NAME,
       CONCAT('[', GROUP_CONCAT(JSON_OBJECT('name', COLUMN_NAME,
                                            'data_type', DATA_TYPE,
                                            'data_length', DATA_LENGTH,
                                            'ordinal_position', ORDINAL_POSITION,
                                            'column_key', COLUMN_KEY)), ']') COLUMNS
  FROM (SELECT c.TABLE_NAME,
               COLUMN_NAME,
               DATA_TYPE,
               CASE
                 WHEN DATA_TYPE = 'varchar'
                 THEN CHARACTER_MAXIMUM_LENGTH
                 WHEN DATA_TYPE IN ('text', 'longtext', 'mediumtext','text') THEN '4000'
                 WHEN DATA_TYPE = 'decimal' THEN CONCAT('(', NUMERIC_PRECISION,',',  NUMERIC_SCALE, ')')
                 ELSE ''
               END DATA_LENGTH,
               ORDINAL_POSITION,
               COALESCE(COLUMN_KEY, '') COLUMN_KEY
          FROM information_schema.TABLES t
          JOIN information_schema.COLUMNS c
            ON (    c.TABLE_SCHEMA = t.TABLE_SCHEMA
                AND c.TABLE_NAME   = t.TABLE_NAME
                AND t.TABLE_TYPE   = 'BASE TABLE'
                AND c.TABLE_SCHEMA = database())
      ) a
 GROUP BY 1;
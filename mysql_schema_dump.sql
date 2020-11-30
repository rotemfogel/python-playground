SELECT TABLE_NAME,
       CONCAT('[', GROUP_CONCAT(JSON_OBJECT('name', COLUMN_NAME,
                                            'data_type', DATA_TYPE,
                                            'ordinal_position', ORDINAL_POSITION,
                                            'column_key', COLUMN_KEY)), ']') COLUMNS
  FROM (SELECT c.TABLE_NAME,
               COLUMN_NAME,
               DATA_TYPE,
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
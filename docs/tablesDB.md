# Notas rápidas de implementación:

`fact_game_achievement_summary` es calculada en pipeline, no extraída directa.

`fact_user_owned_game` y `fact_user_recent_play` son tablas de snapshot, así que define desde el principio si harás inserción por fecha o deduplicación por llave compuesta.

`supported_languages_raw` queda bien como deuda técnica de v1; en v2 puedes normalizarla a `dim_language` + `bridge_game_language`.

`fact_game_review_detail.author_last_played` podría bajarse a DATE si quieres más anonimización y menos granularidad.
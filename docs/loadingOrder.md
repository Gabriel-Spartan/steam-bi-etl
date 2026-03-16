# Orden para cargar datos

El orden importa porque hay llaves foráneas entre tablas.

Aquí la **secuencia lógica**:

## 1. Dimensiones independientes
Estas no dependen de nada más y son base para referencias:

1. `dim_country` ✓
2. `dim_currency` ✓
3. `dim_language` ✓
4. `dim_date` ✓
5. `dim_time_bucket` ✓

## 2. Dimensiones de catálogo
No dependen de hechos, pero se usan más tarde:

6. `dim_genre` ✓
7. `dim_category` ✓
8. `dim_developer` ✓
9. `dim_publisher` ✓

## 3. Dimensiones de catálogo
Que dependen parcialmente de otras:

10. `dim_game`
11. `dim_user`
12. `dim_achievement`

## 4. Bridge tables (M:N)
Conectan dimensiones entre sí:

13. `bridge_game_genre`
14. `bridge_game_category`
15. `bridge_game_developer`
16. `bridge_game_publisher`
17. `bridge_game_language`

## 5. Fact tables
Finalmente cargar los hechos

18. `fact_user_owned_game`
19. `fact_user_recent_play`
20. `fact_user_library_snapshot`
21. `fact_achievement_global`
22. `fact_game_achievement_summary`
23. `fact_concurrent_players_snapshot`kMvc.perform(post("/product")
                        .contentType(MediaType.APPLIC
24. `fact_news_events`
25. `fact_game_price_snapshot`
26. `fact_game_price_period`
27. `fact_game_review_summary`
28. `fact_game_review_detail`

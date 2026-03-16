# Steam Web API - Endpoints útiles

## Variables usadas
- STEAM_ID64 = 76561199070045676
- GAME_ID / APPID = 1030300
- API_KEY = YOUR_STEAM_API_KEY

---

## 1) Catálogo base de juegos
### IStoreService/GetAppList/v1
Devuelve el catálogo público de apps de Steam. Útil para `dim_game` mínima: `appid`, `name`, `last_modified`, `price_change_number`.
```text
https://api.steampowered.com/IStoreService/GetAppList/v1/?key=YOUR_STEAM_API_KEY&include_games=true&include_dlc=true&max_results=50000
```

**Parámetros útiles:**
- `key` = requerido
- `include_games` = true/false
- `include_dlc` = true/false
- `include_software` = true/false
- `include_videos` = true/false
- `include_hardware` = true/false
- `if_modified_since` = epoch opcional
- `last_appid` = para paginación
- `max_results` = hasta 50000

---

## 2) Juegos concurrentes actuales
### ISteamUserStats/GetNumberOfCurrentPlayers/v1
Devuelve cuántas personas están jugando un juego en este momento. Sirve para `fact_concurrent_players_snapshot`.
```text
https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=1030300&format=json
```

**Parámetros:**
- `appid` = requerido
- `format=json` = opcional

---

## 3) Noticias / eventos de un juego
### ISteamNews/GetNewsForApp/v2
Devuelve noticias, anuncios o patch notes de un juego. Sirve para `fact_news_events`.
```text
https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/?appid=1030300&count=10&maxlength=300&format=json
```

**Parámetros:**
- `appid` = requerido
- `count` = número de noticias
- `maxlength` = longitud máxima del texto
- `enddate` = epoch opcional
- `tags` = filtro opcional
- `format=json`

---

## 4) Esquema de logros y stats del juego
### ISteamUserStats/GetSchemaForGame/v2
Devuelve el catálogo de logros y stats definidos para un juego. Sirve para `dim_achievement`.
```text
https://api.steampowered.com/ISteamUserStats/GetSchemaForGame/v2/?key=YOUR_STEAM_API_KEY&appid=1030300&l=english&format=json
```

**Parámetros:**
- `key` = requerido
- `appid` = requerido
- `l` = idioma opcional (english, spanish, etc.)
- `format=json`

---

## 5) Porcentaje global de desbloqueo de logros
### ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2
Devuelve el porcentaje global de usuarios que han desbloqueado cada logro. Sirve para `fact_achievement_global`.
```text
https://api.steampowered.com/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2/?gameid=1030300&format=json
```

**Parámetros:**
- `gameid` = requerido
- `format=json`

---

## 6) Perfil básico del usuario
### ISteamUser/GetPlayerSummaries/v2
Devuelve resumen del perfil del usuario. Sirve para `dim_user` anonimizada.
```text
https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/?key=YOUR_STEAM_API_KEY&steamids=76561199070045676&format=json
```

**Parámetros:**
- `key` = requerido
- `steamids` = requerido (uno o varios separados por coma)
- `format=json`

---

## 7) Juegos jugados recientemente
### IPlayerService/GetRecentlyPlayedGames/v1
Devuelve los juegos jugados recientemente por el usuario. Sirve para `fact_user_recent_play`.
```text
https://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v1/?key=YOUR_STEAM_API_KEY&steamid=76561199070045676&count=7&format=json
```

**Parámetros:**
- `key` = requerido
- `steamid` = requerido
- `count` = requerido en docs; usar 7 o 0
- `format=json`

---

## 8) Biblioteca completa del usuario
### IPlayerService/GetOwnedGames/v1
Devuelve la biblioteca del usuario y tiempos de juego. Sirve para `fact_user_owned_game` y `fact_user_library_snapshot`.
```text
https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/?key=YOUR_STEAM_API_KEY&steamid=76561199070045676&include_appinfo=1&include_played_free_games=1&include_extended_appinfo=1&language=spanish&format=json
```

**Parámetros:**
- `key` = requerido
- `steamid` = requerido
- `include_appinfo=1` = nombres e iconos
- `include_played_free_games=1` = incluye F2P jugados
- `include_extended_appinfo=1` = más metadatos
- `language=spanish` = idioma
- `format=json`

---

## 9) Tiempo de juego de un solo juego para un usuario
### IPlayerService/GetSingleGamePlaytime/v1
Devuelve el tiempo de juego de un usuario para un solo appid.
```text
https://api.steampowered.com/IPlayerService/GetSingleGamePlaytime/v1/?key=YOUR_STEAM_API_KEY&steamid=76561199070045676&appid=1030300&format=json
```

**Parámetros:**
- `key` = requerido
- `steamid` = requerido
- `appid` = requerido
- `format=json`

---

## 10) Wishlist - cantidad
### IWishlistService/GetWishlistItemCount/v1
Devuelve la cantidad de juegos en wishlist del usuario si está accesible.
```text
https://api.steampowered.com/IWishlistService/GetWishlistItemCount/v1/?steamid=76561199070045676
```

**Parámetros:**
- `steamid` = requerido

---

## 11) Wishlist - lista completa
### IWishlistService/GetWishlist/v1
Devuelve la wishlist del usuario si está accesible.
```text
https://api.steampowered.com/IWishlistService/GetWishlist/v1/?steamid=76561199070045676
```

**Parámetros:**
- `steamid` = requerido

---

## 12) Nivel de Steam del usuario
### IPlayerService/GetSteamLevel/v1
Devuelve el nivel de Steam del usuario.
```text
https://api.steampowered.com/IPlayerService/GetSteamLevel/v1/?key=YOUR_STEAM_API_KEY&steamid=76561199070045676&format=json
```

**Parámetros:**
- `key` = requerido
- `steamid` = requerido
- `format=json`

---

## 13) Insignias del usuario
### IPlayerService/GetBadges/v1
Devuelve insignias del usuario.
```text
https://api.steampowered.com/IPlayerService/GetBadges/v1/?key=YOUR_STEAM_API_KEY&steamid=76561199070045676&format=json
```

**Parámetros:**
- `key` = requerido
- `steamid` = requerido
- `format=json`

---

## 14) Lista de endpoints soportados
### ISteamWebAPIUtil/GetSupportedAPIList/v1
Devuelve el catálogo de interfaces y métodos disponibles.
```text
https://api.steampowered.com/ISteamWebAPIUtil/GetSupportedAPIList/v1/?key=YOUR_STEAM_API_KEY&format=json
```

**Parámetros:**
- `key` = opcional
- `format=json`

---

## Qué datos te da cada uno, en corto
`GetAppList` da catálogo base y cambios de apps. `GetNumberOfCurrentPlayers` da demanda actual por juego. `GetNewsForApp` da eventos/noticias. `GetSchemaForGame` da definición de logros y stats. `GetGlobalAchievementPercentagesForApp` da dificultad/progreso global por logro. `GetPlayerSummaries` da perfil básico. `GetRecentlyPlayedGames`, `GetOwnedGames` y `GetSingleGamePlaytime` dan actividad y biblioteca por usuario. `GetWishlist*`, `GetSteamLevel` y `GetBadges` son complementarios y dependen más de privacidad/disponibilidad.

# Steam Store API - appdetails y appreviews

---

## 1) ¿Qué te da `appdetails`?

### Endpoint
```text
https://store.steampowered.com/api/appdetails?appids=1030300&cc=ec&l=spanish
```

### Qué puedes sacar de ahí

#### Identidad base del juego
- `type`
- `name`
- `steam_appid`
- `required_age`
- `is_free`

Sirve para enriquecer `dim_game`. El endpoint `appdetails` devuelve precisamente estos campos junto a muchos otros metadatos del producto.

---

#### Descripciones y contenido textual
- `detailed_description`
- `about_the_game`
- `short_description`

Útiles para metadata, búsqueda semántica o futuras clasificaciones por NLP. `appdetails` incluye estas claves dentro de la respuesta.

---

#### Idiomas soportados
- `supported_languages`

Permite construir una columna o dimensión de idiomas disponibles. Steam documenta `supported_languages` como campo del resultado.

---

#### Requisitos técnicos
- `pc_requirements`
- `mac_requirements`
- `linux_requirements`

Útil para análisis técnico o compatibilidad por plataforma. Los requisitos por SO aparecen como campos del resultado.

---

#### Developer / Publisher
- `developers`
- `publishers`

Muy útiles para `dim_developer`, `dim_publisher` y tablas puente. Steam documenta ambos como arrays de strings dentro de `appdetails`.

---

#### Precio y descuento
- `price_overview.currency`
- `price_overview.initial`
- `price_overview.final`
- `price_overview.discount_percent`
- `package_groups`
- `packages`

Permite crear una fact de precio por snapshot. `price_overview` incluye moneda, precio antes de descuento, precio final y porcentaje de descuento.

---

#### Plataforma
- `platforms.windows`
- `platforms.mac`
- `platforms.linux`

Permite construir una dimensión o flags por plataforma. Steam documenta `platforms` con booleanos para Windows, Mac y Linux.

---

#### Géneros y categorías
- `genres[].id`
- `genres[].description`
- `categories[].id`
- `categories[].description`

Clave para `dim_genre`, `dim_category` y tablas puente. `appdetails` incluye ambas listas con `id` y `description`.

---

#### Popularidad y engagement visibles en tienda
- `recommendations.total`
- `achievements.total`
- `achievements.highlighted`

Da una señal de volumen/atracción del juego en la tienda y total de logros visibles. Steam documenta `recommendations.total` y `achievements.total/highlighted`.

---

#### Lanzamiento
- `release_date.coming_soon`
- `release_date.date`

Sirve para cohortes, análisis temporal y edad del juego. Steam documenta que `release_date` trae `coming_soon` y una fecha localizada según `cc`.

---

#### Imágenes y multimedia
- `header_image`
- `capsule_image`
- `screenshots`
- `movies`
- `background`

No esencial para OLAP, pero sí para dashboards, catálogos o interfaces visuales. `appdetails` documenta screenshots, movies y assets visuales.

---

#### Metacritic y ratings
- `metacritic.score`
- `metacritic.url`
- `ratings`
- `content_descriptors`

Sirven para enriquecer análisis de percepción externa o clasificación de contenido. `appdetails` documenta `metacritic` y otros metadatos opcionales.

---

## 2) ¿Qué te da `appreviews`?

### Endpoint
```text
https://store.steampowered.com/appreviews/1030300?json=1&language=all&purchase_type=all&filter=all&day_range=365&cursor=*
```

### Qué puedes sacar de ahí

#### Resumen agregado

Dentro de `query_summary`:

- `num_reviews`
- `review_score`
- `review_score_desc`
- `total_positive`
- `total_negative`
- `total_reviews`

Permite crear una fact agregada de reseñas por snapshot. Steam documenta todos estos campos en `query_summary`.

---

#### Cursor para paginar
- `cursor`

Sirve para seguir trayendo más reseñas. Steam documenta que debes pasar `*` en la primera llamada y luego reutilizar el cursor devuelto.

---

#### Reseñas individuales

Dentro de `reviews[]`:

- `recommendationid`
- `language`
- `review`
- `timestamp_created`
- `timestamp_updated`
- `voted_up`
- `votes_up`
- `votes_funny`
- `weighted_vote_score`
- `comment_count`
- `steam_purchase`
- `received_for_free`
- `refunded`
- `written_during_early_access`
- `primarily_steam_deck`

Todo esto es útil para una tabla detallada de reseñas. Steam documenta `recommendationid`, el texto, timestamps, polaridad, votos y flags de compra/EA.

---

#### Bloque autor

Dentro de `author`:

- `steamid`
- `num_games_owned`
- `num_reviews`
- `playtime_forever`
- `playtime_last_two_weeks`
- `playtime_at_review`
- `last_played`

Da contexto de la reseña: si quien reseña jugó mucho o casi nada, si es comprador de Steam o no, si jugó recientemente, etc. Steam documenta este bloque como parte del retorno de `reviews`.

---

#### Filtros útiles del endpoint

| Parámetro | Opciones |
|---|---|
| `filter` | `recent`, `updated`, `all` |
| `language` | código de idioma o `all` |
| `day_range` | hasta `365` |
| `review_type` | `all`, `positive`, `negative` |
| `purchase_type` | `all`, `steam`, `non_steam_purchase` |
| `num_per_page` | hasta `100` |
| `filter_offtopic_activity` | `0` para incluir review bombs |

Steam documenta estos parámetros y sus significados.

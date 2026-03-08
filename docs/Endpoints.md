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
# Origen de datos

## Endpoints oficiales útiles para BI (Steam Web API)

1. Catálogo de apps (seed de dim_game)

`IStoreService/GetAppList/v1`

Para qué sirve: obtener lista de apps disponibles (appid, nombre) y campos como last_modified/price_change_number (útil para saber qué apps cambiaron y refrescar otras extracciones).

URL
```
https://api.steampowered.com/IStoreService/GetAppList/v1/
```
Parámetros

**Requeridos**

`key` (string)

Opcionales recomendados

include_games (bool) — por defecto suele venir habilitado

include_dlc (bool)

include_software (bool)

include_videos (bool)

include_hardware (bool)

if_modified_since (uint32, epoch) — para traer solo cambios

last_appid (uint32) — paginación/continuación

max_results (uint32, máx 50k)

format=json

2. Jugadores concurrentes actuales (ideal para snapshots por hora/día)
ISteamUserStats/GetNumberOfCurrentPlayers/v1

Para qué sirve: devuelve el total de jugadores actualmente activos en un appid. Para ver “por hora” o “por día”, tú lo conviertes en histórico guardando snapshots periódicos.

URL
```
https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/
```

Parámetros

**Requeridos**

`appid (uint32)`

Opcionales

format=json

key (string) (normalmente no es necesario en el host público, pero puedes incluirlo)

Nota BI: esto NO devuelve histórico; el histórico lo construyes con tu pipeline guardando el resultado cada X minutos/horas. Además, no cuenta jugadores “offline/no conectados a Steam”.

3. Noticias/Anuncios/Patch notes (contexto para picos)
ISteamNews/GetNewsForApp/v2 (o v1)

Para qué sirve: obtener posts/noticias por juego (útil para correlacionar con picos de jugadores o cambios de reseñas).

URL
```
https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/
```
Parámetros

**Requeridos**

`appid (uint32)`

Opcionales recomendados

count (uint32) — cuántas entradas traer

maxlength (uint32) — largo del contenido devuelto

enddate (uint32, epoch) — paginar hacia atrás

feeds (string) — filtrar por tipo/feed (ej. community announcements)

tags (string) — filtrar por tags (ej. patchnotes)

format=json

4. Listado de endpoints disponibles (para “gobernanza” y alcance)
ISteamWebAPIUtil/GetSupportedAPIList/v1

Para qué sirve: descubrir interfaces/métodos y sus parámetros (sirve para justificar alcance y no “prometer” datos inexistentes).

URL
```
https://api.steampowered.com/ISteamWebAPIUtil/GetSupportedAPIList/v1/
```
Parámetros

Opcionales

key (string) — para recibir métodos restringidos si aplica

format=json

Endpoints oficiales útiles SOLO si tu BI incluye “usuario” (SteamID)

Estos NO son para mercado global; son para análisis personalizado (tu cuenta o usuarios que acepten compartir).

5. Resolver vanity URL → SteamID64
ISteamUser/ResolveVanityURL/v1

Para qué sirve: si un perfil es /id/usuario, lo convierte a SteamID64.

Parámetros

**Requeridos**

``key (string)``

``vanityurl (string)``

Opcionales

url_type (int32)

format=json

6. Owned Games + playtime (por usuario)
IPlayerService/GetOwnedGames/v1 (requiere SteamID64)

Para qué sirve: librería del usuario y tiempo jugado (si el perfil lo permite). Útil para “perfil del jugador”, no para mercado completo.

Parámetros

**Requeridos**

key (string)

steamid (uint64)

Opcionales recomendados

include_appinfo (bool)

include_played_free_games (bool)

appids_filter (uint32 / lista según implementación)

language (string)

format=json

7) Wishlist del usuario
IWishlistService/GetWishlist/v1 (requiere SteamID64)

Para qué sirve: lista de deseados del usuario (si está accesible). Útil para personalización; no es métrica global del mercado.

Parámetros

Requeridos

steamid (uint64)

Opcionales

(para GetWishlistSortedFiltered) paginación/orden/filtros
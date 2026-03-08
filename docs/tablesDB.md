Tablas que puedes tener “por el momento” (mercado + sin depender de steamid)
1) dim_game_min (mínima)

Fuente: IStoreService/GetAppList
Campos mínimos recomendados:

game_key (PK surrogate)

appid (natural key)

name

last_modified (timestamp)

price_change_number

Uso BI: catálogo base + control de cambios (saber qué app cambió y volver a consultar otras fuentes).

2) fact_concurrent_players_snapshot

Fuente: ISteamUserStats/GetNumberOfCurrentPlayers (appid → player_count)
Mejor grano recomendado:

1 fila por appid por “instante de captura” (timestamp).
Es decir: appid + captured_at como clave única.

Columnas:

appid

captured_at (timestamp)

player_count

¿Por hora o por día?

Lo ideal es por hora (te da curva diaria real).

Luego agregas en OLAP a por día con AVG/MAX/MIN (ej. pico diario = MAX).
Así no pierdes información.

3) fact_news_events (opcional)

Fuente: ISteamNews/GetNewsForApp (no requiere steamid; requiere appid)
Grano recomendado:

1 fila por noticia/post (por gid o id que venga en la respuesta).

Columnas típicas:

appid

news_id / gid

date_published

title

feedlabel / tags (si vienen)

url

Uso BI: explicar picos o caídas: “hubo update/announcement” vs “subieron jugadores”.
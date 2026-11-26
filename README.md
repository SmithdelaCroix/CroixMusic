# Croix Player (0.5 beta)

Reproductor de musica de codigo abierto para Android 12 o superior (solo arquitectura `arm64-v8a`). Usa YouTube Music como fuente de audio sin anuncios y permite conectar tu cuenta de YouTube Music y de Spotify.

Esta en version **0.5 beta**, asi que puede haber cosas por pulir.

## Descarga

- APK directo (0.5 beta): [CroixPlayer-arm64-v8a.apk](https://github.com/SmithdelaCroix/CroixMusic/releases/latest/download/CroixPlayer-arm64-v8a.apk)
- Versiones publicadas: https://github.com/SmithdelaCroix/CroixMusic/releases

Requisitos: Android 12+ y telefono de 64 bits (`arm64-v8a`).

## Funciones

### Reproduccion y cola
- Streaming de audio desde YouTube Music sin anuncios.
- Cambio de origen entre conexion directa, nodos Piped o un servidor propio.
- Descarga de canciones para escuchar sin internet y limite de cache configurable.
- Colas largas (150 a 200+ canciones) que se van generando solas cuando te acercas al final.
- Boton de aleatorio con varios modos: orden normal, aleatorio, repetir una sola cancion y mezcla equilibrada (favoritas, conocidas y nuevas).
- Edicion de la cola: subir o bajar canciones, quitar duplicados y guardar la cola como playlist.
- Control de velocidad, tono (pitch), saltar silencios, crossfade, temporizador de apagado y SponsorBlock para saltar partes que no son musica.

### Cuentas, Spotify y recomendaciones
- Inicio de sesion con YouTube Music para sincronizar Me gusta, historial y playlists.
- Cambio entre varias cuentas de Google guardadas y cambio entre canales o perfiles dentro del mismo correo.
- Conexion con cuenta de Spotify para sincronizar artistas, canciones escuchadas, Me gusta, playlists y mixes (Daily Mix, Descubrimiento Semanal, Daylist, Radar de Novedades).
- Reproduccion directa o importacion de playlists y mixes de Spotify, ademas de importacion por enlace.
- Recomendaciones basadas en historial y artistas relacionados, con control para mezclar el peso entre YouTube Music y Spotify. Filtra covers, versiones slowed, 8D, karaoke o recopilaciones largas.
- Pantalla de inicio con Flow por estado de animo, Joyas ocultas (canciones que no escuchaste), mixes diarios, seleccion rapida y artistas recomendados.
- Sesion privada (pausa el historial, estadisticas y scrobbling) y lista de artistas bloqueados.

### Letras, busqueda e integraciones
- Letras sincronizadas y simples desde LRCLib, Spotify y YouTube Music, con ajuste manual de sincronizacion, romanizacion, traduccion al espanol y opcion de compartir versos como imagen.
- Reconocimiento de canciones usando el microfono.
- Buscador por canciones, artistas y albumes, con pagina de artista completa (temas populares, albumes y similares).
- Integracion opcional con API de IA (Gemini u OpenAI) para traducir letras y pedir recomendaciones segun tu historial.
- Soporte para Discord Rich Presence, Last.fm y ListenBrainz.

### Audio y ecualizador
- Ecualizador de 5 bandas con preajustes y perfiles AutoEq para distintos audifonos.
- Cambio automatico de perfil de ecualizacion segun la salida de audio (Bluetooth, cable o altavoz).
- Refuerzo de graves, ganancia de volumen, reverb y audio 8D.

### Biblioteca y personalizacion
- Favoritos con vista en lista o cuadricula, buscador y orden por recientes, titulo o artista.
- Playlists locales, historial de reproduccion y estadisticas de escucha con tarjeta para compartir.
- Temas predefinidos, color manual o dinamico segun la portada, modo AMOLED, transparencia y bordes ajustables.
- Opciones para cambiar el estilo del reproductor, fondo, forma de la portada (incluyendo disco de vinilo), barra de progreso, minirreproductor, barra de navegacion, fuentes, animaciones de letras e icono de la app.
- Gestos configurables (deslizar portada, doble toque para adelantar o retroceder, agitar para cambiar cancion).
- Exportar e importar el tema en formato JSON.

### Funciones beta
Se activan manualmente en `Configuracion > Funciones BETA` (vienen apagadas por defecto):
- Caratula 3D con giroscopio.
- Modo misterio (oculta la portada y el nombre durante los primeros 45 segundos y los revela de a poco).
- Modo karaoke (reduce la voz en tiempo real y abre las letras).

## Compilar desde codigo fuente

Necesitas JDK 21 y Android SDK 35:

```bash
./gradlew :app:assembleRelease
```

## Licencia

GPL-3.0. Revisa el archivo [LICENSE](LICENSE).

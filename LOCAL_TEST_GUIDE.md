# Local test guide: python-loader-chaos + MetaDataCube frontend

How to load a few of your own photos, let the plugins (exif → time-info / geodatas) tag them, and browse the result in the MetaDataCube frontend.

Folders used below
- `python-loader-chaos/`: backend, plugins, CLI (`client/`)
- `PhotoCube-Server/`: the M3 server (REST API over the database)
- `metadatacube-client_2024/`: the frontend

---

## 0. One-time setup (first run)

| What | Why |
|---|---|
| `STANDALONE_MODE: "true"` on `exif-extractor` in `docker-compose.yaml` | There is no media-orchestrator in the compose file, so exif must listen for new media itself. |
| `git clone https://github.com/Ok2610/PhotoCube-Server.git` then `docker build -t m3-server .` inside it | Builds the M3 server image (it needs .NET 9, so we run it in Docker instead of installing it). |
| Replace `metadatacube-client_2024/src/proxy.conf.json` with the version below | Sends `/api` to the M3 server and `/app/medias/...` image paths to the image server. |
| `nvm install 20` | Angular 17 needs Node ≥ 18.13. |
| `cd metadatacube-client_2024 && npm install` | Installs the frontend's dependencies. |

`proxy.conf.json`:
```json
{
  "/api": { "target": "http://localhost:5002", "secure": false },
  "/app/medias": {
    "target": "http://localhost:5005",
    "secure": false,
    "pathRewrite": { "^/app/medias": "" }
  }
}
```

---

## 1. Choose the data

Copy your photos into a folder inside `python-loader-chaos/medias/`, e.g. `medias/test_folder/`.

- Only `medias/` is visible to the Docker containers (mounted as `/app/medias`).
- Use **JPEG files with a lowercase `.jpg` extension** (rename `.jpeg`/`.JPG`).
- Photos straight from a phone/camera have EXIF date + GPS; screenshots and downloaded images usually don't.

---

## 2. Start python-loader-chaos

In `python-loader-chaos/`:

```bash
docker compose up --build
```
`--build` rebuilds the images so code changes (e.g. to a plugin) are picked up; without it, images that already exist are reused. Builds and starts the database, RabbitMQ, go-server, media-downloader and plugins, and shows their logs in this terminal. Leave it running and use a new terminal for the next steps. Wait until the logs calm down (go-server is healthy) before continuing.

---

## 3. Prepare the database

In `python-loader-chaos/client/`, with the conda env active (`conda activate python-loader`):

```bash
python ./cli.py reset
```
Empties the database (the data survives restarts, so do this to start from scratch).

```bash
python ./cli.py import_tagsets --format csv ./LscTagsetFile.csv
```
Creates the standard tagsets, including `Timestamp UTC`, which exif needs for the photo date.

```bash
python ./cli.py add tagset Location 1
```
Creates the `Location` tagset (type 1) so exif can save GPS, which in turn triggers geodatas.

---

## 4. Insert the photos

Optional, in a second terminal, to watch the plugin work:
```bash
docker logs -f --since 1m plugin-exif-extractor
```

From any folder (change `test_folder` to your folder name):
```bash
docker exec python-client python cli.py add medias /app/medias/test_folder
```
Runs the CLI inside the `python-client` container, which sees `medias/` at the same `/app/medias` path as the plugins, so each photo is stored with a path the plugins can open. Prints the new media IDs. (Running `add medias` on your Mac would store your Mac path, which the containers can't read.) Adding the same path twice fails, so reset first if you re-run.

---

## 5. Start the frontend and browse

Each command in its own terminal; leave them running.

**M3 server** (any folder):
```bash
docker run --rm --name m3-server --network python-loader-chaos_default -p 5002:5002 -e ASPNETCORE_ENVIRONMENT=Development -e "ConnectionStrings__DefaultConnection=Server=db;Port=5432;Database=loader-testing;User Id=postgres;Password=root;" m3-server
```
Runs the REST API the frontend talks to, on the loader's Docker network so it reaches the `db` container. It prints almost nothing; check http://localhost:5002/api/tagset shows your tagsets.

**Image server**, in `python-loader-chaos/`:
```bash
python -m http.server 5005 --directory medias
```
Serves the photos so the browser can load `/app/medias/...` paths (via the proxy).

**Frontend**, in `metadatacube-client_2024/`:
```bash
nvm use 20
```
```bash
npm start
```
Starts Angular with the proxy config. Open http://localhost:4200, pick e.g. `Country`, `Day of week (string)` or the `Location` hierarchy as an axis. Press **Reload Data** after adding new images/tags (the tagset list is cached in the browser).

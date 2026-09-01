#!/usr/bin/env node
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { createServer } from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const webRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const distDirectory = path.resolve(webRoot, process.argv[2] ?? "apps/xflow-admin/dist");
const port = Number.parseInt(process.env.PORT ?? "4173", 10);
const host = process.env.HOST ?? "127.0.0.1";
const indexPath = path.join(distDirectory, "index.html");

const contentTypes = new Map([
  [".css", "text/css; charset=utf-8"],
  [".html", "text/html; charset=utf-8"],
  [".ico", "image/x-icon"],
  [".jpeg", "image/jpeg"],
  [".jpg", "image/jpeg"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".map", "application/json; charset=utf-8"],
  [".mjs", "text/javascript; charset=utf-8"],
  [".png", "image/png"],
  [".svg", "image/svg+xml"],
  [".webp", "image/webp"]
]);

function sendFile(request, response, filePath, size) {
  response.writeHead(200, {
    "content-length": size,
    "content-type": contentTypes.get(path.extname(filePath).toLowerCase()) ?? "application/octet-stream",
    "x-content-type-options": "nosniff"
  });
  if (request.method === "HEAD") response.end();
  else createReadStream(filePath).pipe(response);
}

async function regularFile(filePath) {
  try {
    const fileStat = await stat(filePath);
    return fileStat.isFile() ? fileStat : undefined;
  } catch (error) {
    if (error?.code === "ENOENT" || error?.code === "ENOTDIR") return undefined;
    throw error;
  }
}

await regularFile(indexPath).then((fileStat) => {
  if (!fileStat) throw new Error(`Missing production entry point: ${indexPath}`);
});

const server = createServer(async (request, response) => {
  try {
    if (request.method !== "GET" && request.method !== "HEAD") {
      response.writeHead(405, { allow: "GET, HEAD" }).end();
      return;
    }

    let pathname;
    try {
      pathname = decodeURIComponent(new URL(request.url ?? "/", `http://${host}`).pathname);
    } catch {
      response.writeHead(400).end("Bad request");
      return;
    }

    const requestedPath = path.resolve(distDirectory, pathname.replace(/^[/\\]+/, ""));
    const insideDist =
      requestedPath === distDirectory || requestedPath.startsWith(`${distDirectory}${path.sep}`);
    if (!insideDist) {
      response.writeHead(403).end("Forbidden");
      return;
    }

    const requestedFile = await regularFile(requestedPath);
    if (requestedFile) {
      sendFile(request, response, requestedPath, requestedFile.size);
      return;
    }

    const acceptsHtml = (request.headers.accept ?? "").includes("text/html");
    if (acceptsHtml || path.extname(pathname) === "") {
      const indexStat = await stat(indexPath);
      sendFile(request, response, indexPath, indexStat.size);
      return;
    }

    response.writeHead(404).end("Not found");
  } catch (error) {
    console.error(error);
    if (!response.headersSent) response.writeHead(500);
    response.end("Internal server error");
  }
});

server.listen(port, host, () => {
  console.log(`Serving ${path.relative(webRoot, distDirectory)} at http://${host}:${port}`);
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => server.close(() => process.exit(0)));
}

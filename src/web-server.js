import express from "express";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { History } from "./history.js";
import { DemoService, DemoError } from "./demo-service.js";
import { kafka, TOPIC } from "./transport.js";

export function createWebApp(service) {
  const app = express();
  const instanceId = randomUUID();
  const listeners = new Set();
  let revision = 0;
  let broker = { status: "checking", checked_at: null };
  let refreshing;
  let latest;
  let latestError;
  const notify = (event, data) => {
    for (const res of listeners)
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };
  const refresh = () => {
    refreshing ??= (async () => {
      try {
        latest = {
          ...(await service.snapshot()),
          broker,
          database: { status: "connected" },
          revision: ++revision,
          instance_id: instanceId,
        };
        latestError = null;
        notify("state", latest);
        return latest;
      } catch (error) {
        latestError = error;
        notify("unavailable", {
          error:
            "The event history is unavailable. Check PostgreSQL and try again.",
        });
        throw error;
      }
    })().finally(() => {
      refreshing = null;
    });
    return refreshing;
  };
  app.disable("x-powered-by");
  app.use("/api", (req, res, next) => {
    res.set("Cache-Control", "no-store");
    if (req.method === "POST") {
      if (!req.is("application/json"))
        return res.status(415).json({ error: "Send JSON for this action." });
      const origin = req.get("origin");
      let sameOrigin = false;
      try {
        sameOrigin = !!origin && new URL(origin).host === req.get("host");
      } catch {
        /* Invalid origins are not trusted. */
      }
      if (origin && !sameOrigin) {
        return res.status(403).json({
          error: "Open the demo locally before performing this action.",
        });
      }
    }
    next();
  });
  app.use(express.json({ limit: "16kb" }));
  app.get("/api/state", async (_req, res) => res.json(await refresh()));
  app.get("/api/stream", async (req, res) => {
    res.set({
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    res.flushHeaders();
    listeners.add(res);
    if (latest) res.write(`event: state\ndata: ${JSON.stringify(latest)}\n\n`);
    if (latestError)
      res.write(
        'event: unavailable\ndata: {"error":"Event history is unavailable."}\n\n',
      );
    req.on("close", () => listeners.delete(res));
    try {
      await refresh();
    } catch {
      /* Stream reports unavailable and retries on the next tick. */
    }
  });
  const action = (work) => async (req, res) => {
    const result = await work(req);
    await refresh();
    res.json({ result });
  };
  app.post(
    "/api/scenarios",
    action(() => service.startScenario()),
  );
  app.post(
    "/api/samples",
    action(() => service.addSamples()),
  );
  app.post(
    "/api/scenarios/:id/select",
    action((req) => service.selectScenario(req.params.id)),
  );
  app.post(
    "/api/scenarios/:id/events",
    action((req) => service.send(req.params.id, req.body || {})),
  );
  app.post(
    "/api/scenarios/:id/events/:eventId/retry",
    action((req) => service.retry(req.params.id, req.params.eventId)),
  );
  app.post(
    "/api/scenarios/:id/pull",
    action((req) => service.pull(req.params.id)),
  );
  app.post(
    "/api/scenarios/:id/review",
    action((req) => service.review(req.params.id)),
  );
  app.get("/api/scenarios/:id/events/:eventId/evidence", async (req, res) =>
    res.json(await service.evidence(req.params.id, req.params.eventId)),
  );
  app.get("/api/evidence", (_req, res) =>
    res.json({
      content: readFileSync(
        new URL("../fixtures/evidence.txt", import.meta.url),
        "utf8",
      ),
    }),
  );
  app.use("/api", (_req, res) =>
    res.status(404).json({ error: "Unknown demo action." }),
  );
  const staticPath = fileURLToPath(new URL("../dist/web/", import.meta.url));
  app.use(express.static(staticPath));
  app.get(["/", "/humata", "/flow"], (_req, res) =>
    res.sendFile(resolve(staticPath, "index.html")),
  );
  app.use((error, _req, res, _next) => {
    const status =
      error instanceof DemoError
        ? error.status
        : error.type === "entity.parse.failed"
          ? 400
          : 503;
    if (status >= 500) console.error(`Web demo: ${error.message}`);
    const message =
      error instanceof DemoError
        ? error.message
        : status === 400
          ? "The request contains invalid JSON."
          : "The demo could not complete this request. Check the service logs and try again.";
    if (!res.headersSent) res.status(status).json({ error: message });
  });
  const poll = setInterval(() => refresh().catch(() => {}), 1000);
  poll.unref();
  let checking = false;
  const checkBroker = async () => {
    if (checking) return;
    checking = true;
    const client = kafka("fiber-web-health", 0).admin();
    try {
      await client.connect();
      const metadata = await client.fetchTopicMetadata({ topics: [TOPIC] });
      if (
        !metadata.topics.find((topic) => topic.name === TOPIC)?.partitions
          .length
      )
        throw new Error("Topic is unavailable");
      broker = { status: "connected", checked_at: new Date().toISOString() };
    } catch {
      broker = { status: "unavailable", checked_at: new Date().toISOString() };
    } finally {
      await client.disconnect();
      checking = false;
    }
  };
  const healthPoll = setInterval(checkBroker, 10000);
  healthPoll.unref();
  return {
    app,
    checkBroker,
    close: () => {
      clearInterval(poll);
      clearInterval(healthPoll);
      for (const res of listeners) res.end();
    },
  };
}

if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  const history = new History();
  await history.init();
  const web = createWebApp(new DemoService(history));
  web.checkBroker().catch((error) => console.error(error.message));
  const port = Number(process.env.PORT || 8088);
  const server = web.app.listen(port, "0.0.0.0", () =>
    console.log(`Fiber demo: http://localhost:${port}`),
  );
  const stop = () => {
    web.close();
    server.close(async () => {
      await history.close();
      process.exit(0);
    });
  };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
}

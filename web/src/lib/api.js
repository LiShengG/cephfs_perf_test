export async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const contentType = response.headers.get("content-type") || "";
  const rawText = await response.text();
  let payload = null;

  if (contentType.includes("application/json")) {
    try {
      payload = rawText ? JSON.parse(rawText) : {};
    } catch (err) {
      throw new Error(`Invalid JSON response from ${path}: ${err.message}`);
    }
  } else {
    const preview = rawText.trim().slice(0, 120);
    if (preview.startsWith("<!DOCTYPE") || preview.startsWith("<html")) {
      throw new Error(
        `API ${path} returned HTML instead of JSON. If you are using Vite dev server, make sure the API proxy is enabled and the backend is running on 127.0.0.1:18080.`
      );
    }
    throw new Error(`API ${path} returned unexpected content type: ${contentType || "unknown"}`);
  }

  if (!response.ok || payload.ok === false) {
    throw new Error(payload.error || `request failed: ${response.status}`);
  }
  return payload;
}

export async function fetchConfigs() {
  return api("/api/configs");
}

export async function fetchConfig(name) {
  return api(`/api/configs/${encodeURIComponent(name)}`);
}

export async function saveConfig(name, config) {
  return api(`/api/configs/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify({ config }),
  });
}

export async function createConfig(name, config) {
  return api("/api/configs", {
    method: "POST",
    body: JSON.stringify({ name, config }),
  });
}

export async function renameConfig(name, newName) {
  return api(`/api/configs/${encodeURIComponent(name)}/rename`, {
    method: "POST",
    body: JSON.stringify({ new_name: newName }),
  });
}

export async function deleteConfig(name, options = {}) {
  const params = new URLSearchParams();
  if (options.force) {
    params.set("force", "1");
  }
  const query = params.toString();
  const path = `/api/configs/${encodeURIComponent(name)}${query ? `?${query}` : ""}`;
  return api(path, { method: "DELETE" });
}

export async function fetchCurrentRun() {
  return api("/api/runs/current");
}

export async function startRun(configName) {
  return api("/api/runs", {
    method: "POST",
    body: JSON.stringify({ config_name: configName }),
  });
}

export async function stopRun() {
  return api("/api/runs/stop", { method: "POST" });
}

export async function fetchRun(configName, runId) {
  const base = `/api/configs/${encodeURIComponent(configName)}/runs/${encodeURIComponent(runId)}`;
  const [run, stdout, stderr, summary, snapshot] = await Promise.all([
    api(base),
    api(`${base}/logs/stdout`),
    api(`${base}/logs/stderr`),
    api(`${base}/summary`),
    api(`${base}/snapshot`),
  ]);
  return {
    run: run.run,
    stdout: stdout.content,
    stderr: stderr.content,
    summary: summary.summary,
    snapshot: snapshot.snapshot,
  };
}

export async function fetchMetricSeries(configName, runId, metric) {
  return api(
    `/api/configs/${encodeURIComponent(configName)}/runs/${encodeURIComponent(runId)}/metrics/series?metric=${encodeURIComponent(metric)}`
  );
}

export async function deleteRun(configName, runId) {
  return api(`/api/configs/${encodeURIComponent(configName)}/runs/${encodeURIComponent(runId)}`, {
    method: "DELETE",
  });
}

# Gerrit Webhooks Plugin – Configuration Guide

## Overview

The Gerrit **webhooks** plugin ships pre-installed with the standard Gerrit Docker image and is
loaded automatically on startup. However, the plugin requires explicit configuration before it
will fire any HTTP requests. This page documents the two supported configuration methods and the
troubleshooting steps used to identify the correct one.

---

## Prerequisites

| Requirement | Details |
|---|---|
| Gerrit version | 3.x (tested on 3.13.5) |
| Plugin | `webhooks.jar` — bundled in the official Gerrit image |
| Admin access | HTTP basic auth or cookie-based session with Administrator capability |
| Target endpoint | An HTTP server reachable from inside the Gerrit container |

---

## How the Plugin Reads Configuration

> **Important:** The webhooks plugin does **NOT** read from `$GERRIT_SITE/etc/webhooks.config`.
> Configuration must be stored in the `refs/meta/config` branch of a Gerrit project.

The plugin supports two configuration scopes:

- **Global** – stored in the `All-Projects` project's `refs/meta/config` branch; inherited by
  every project.
- **Per-project** – stored in an individual project's `refs/meta/config` branch; overrides
  global settings for that project.

Global plugin tunables (timeouts, retry counts, etc.) are stored in `gerrit.config` under
`[plugin "webhooks"]`.

---

## Method 1 – REST API (Recommended)

This is the simplest and most reliable method. No git operations or container restarts are
required.

### Configure a global webhook on All-Projects

```
PUT /a/config/server/webhooks~projects/{project-name}/remotes/{remote-name}
```

**Example – apply to all projects:**

```bash
curl -u "admin:secret" -X PUT \
  "http://<gerrit-host>:8080/a/config/server/webhooks~projects/All-Projects/remotes/<remote-name>" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "http://<webhook-receiver-host>:<port>",
    "events": [
      "patchset-created",
      "change-abandoned",
      "change-restored",
      "change-merged",
      "comment-added",
      "ref-updated",
      "reviewer-added",
      "topic-changed"
    ]
  }'
```

A `201 Created` (or `200 OK` if updating) response confirms success. The configuration is written
immediately into `All-Projects`'s `refs/meta/config` branch — no restart needed.

### Verify the configuration

```bash
curl -u "admin:secret" \
  "http://<gerrit-host>:8080/a/config/server/webhooks~projects/All-Projects/remotes/"
```

Strip the `)]}'\n` XSSI prefix from the response. A correctly configured remote looks like:

```json
{
  "zuul": {
    "url": "http://<webhook-receiver-host>:<port>",
    "events": [
      "patchset-created",
      "change-abandoned",
      "change-restored",
      "change-merged",
      "comment-added",
      "ref-updated",
      "reviewer-added",
      "topic-changed"
    ],
    "connectionTimeout": 5000,
    "socketTimeout": 5000,
    "maxTries": 5,
    "retryInterval": 1000,
    "sslVerify": false
  }
}
```

### Delete a webhook

```bash
curl -u "admin:secret" -X DELETE \
  "http://<gerrit-host>:8080/a/config/server/webhooks~projects/All-Projects/remotes/<remote-name>"
```

---

## Method 2 – Direct git push to refs/meta/config

Use this method if you prefer managing configuration as code or do not have REST API access.

### Steps

**1. Clone the All-Projects meta config branch:**

```bash
git clone "ssh://<admin>@<gerrit-host>:29418/All-Projects" all-projects
cd all-projects
git fetch origin refs/meta/config:refs/meta/config
git checkout refs/meta/config
```

**2. Create or edit `webhooks.config`:**

```ini
[remote "zuul"]
  url = http://<webhook-receiver-host>:<port>
  event = patchset-created
  event = change-abandoned
  event = change-restored
  event = change-merged
  event = comment-added
  event = ref-updated
  event = reviewer-added
  event = topic-changed
```

**3. Commit and push back:**

```bash
git add webhooks.config
git commit -m "Configure webhooks"
git push origin HEAD:refs/meta/config
```

The plugin picks up the change immediately — no restart required.

---

## Supported Event Types

| Event | Triggered when |
|---|---|
| `patchset-created` | A new patch set is uploaded |
| `change-abandoned` | A change is abandoned |
| `change-restored` | An abandoned change is restored |
| `change-merged` | A change is submitted and merged |
| `comment-added` | A reviewer posts a comment or vote |
| `ref-updated` | A ref (branch, tag) is updated directly |
| `reviewer-added` | A reviewer is added to a change |
| `topic-changed` | The topic of a change is updated |
| `hashtags-changed` | Hashtags on a change are modified |
| `project-created` | A new project is created |

If no `event` lines are specified in the remote config, all allowed event types are sent.

---

## Global Plugin Tuning (gerrit.config)

These settings apply to all remotes unless overridden at the remote level.
Add this block to `$GERRIT_SITE/etc/gerrit.config` and reload or restart Gerrit:

```ini
[plugin "webhooks"]
  connectionTimeout = 3000
  socketTimeout     = 2500
  maxTries          = 5
  retryInterval     = 1000
  threadPoolSize    = 3
  sslVerify         = false
```

---

## Reloading the Plugin

After making changes via the REST API or git push, the plugin reacts immediately. To force a
reload (e.g. after editing `gerrit.config`):

```bash
curl -u "admin:secret" -X POST \
  "http://<gerrit-host>:8080/a/plugins/webhooks/gerrit~reload"
```

A `200 OK` response with the plugin info object confirms the reload succeeded.

---

## Docker / Container-specific Notes

When Gerrit runs in Docker Compose, use the **Docker network service name** as the webhook target
host — not `localhost`.

| Context | Correct URL format |
|---|---|
| From host machine | `http://localhost:<port>/...` |
| From inside Gerrit container (same Compose network) | `http://<service-name>:<port>/...` |
| External IP (e.g. WSL bridge or separate host) | `http://172.x.x.x:<port>/...` |

For Zuul CI specifically, the webhook URL follows this pattern:

```
http://<zuul-web-host>:<port>/api/connection/<connection-name>/payload
```

Where `<connection-name>` matches the `[connection "name"]` block in `zuul.conf`.

---

## Troubleshooting

### Webhooks not firing after adding etc/webhooks.config

| | |
|---|---|
| **Symptom** | You created `$GERRIT_SITE/etc/webhooks.config` but no HTTP requests reach the target. |
| **Cause** | The file is silently ignored. The plugin only reads configuration from `refs/meta/config` project branches. |
| **Fix** | Use the REST API method (Method 1) described above. |

### Plugin loads but no outbound requests appear

Check the Gerrit error log inside the container:

```bash
docker exec <gerrit-container> grep -i webhook /var/gerrit/logs/error_log
```

Confirm the plugin loaded:

```
INFO  PluginLoader : Loaded plugin webhooks, version vX.Y.Z
```

If the plugin is loaded but events are silent, the remote is not configured in `refs/meta/config`.
Verify with:

```bash
curl -u "admin:secret" \
  "http://<gerrit-host>:8080/a/config/server/webhooks~projects/All-Projects/remotes/"
```

### Connection refused at target URL

- Confirm the target service is running and bound to `0.0.0.0` (not `127.0.0.1`).
- If using Docker networking, ensure Gerrit and the target are on the same Docker network.
- Test reachability from inside the container:

```bash
docker exec <gerrit-container> curl -v http://<target-host>:<port>
```

---

## References

- [Gerrit webhooks plugin – config.md](https://gerrit.googlesource.com/plugins/webhooks/+/refs/heads/master/src/main/resources/Documentation/config.md)
- [Gerrit webhooks plugin – REST API](https://gerrit.googlesource.com/plugins/webhooks/+/refs/heads/master/src/main/resources/Documentation/rest-api-config.md)
- [Gerrit Plugin Administration](https://gerrit-review.googlesource.com/Documentation/config-plugins.html)

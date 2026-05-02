---
title: Torbox Setup
description: Configure Torbox provider.
---

Torbox is a supported Debrid provider.

## Configuration

```json
{
  "debrids": [
    {
      "provider": "torbox",
      "name": "Torbox",
      "api_key": "YOUR_API_KEY"
    }
  ]
}
```

Get your API key from the Torbox dashboard.

## Torbox Usenet for NZBs

Torbox can also be used as the NZB download provider. Enable the Torbox Usenet backend on the Torbox debrid entry:

```json
{
  "debrids": [
    {
      "provider": "torbox",
      "name": "Torbox",
      "api_key": "YOUR_API_KEY",
      "usenet_backend": "torbox",
      "usenet_post_process": -1
    }
  ]
}
```

Then open **Download** in the web UI, add NZB URLs or files, and set **NZB Provider** to **Torbox**. If direct NNTP providers are also configured, choose **Direct Usenet (NNTP)** to process NZBs locally instead.

`usenet_post_process` is passed to Torbox when the NZB is created. Use `-1` for the Torbox account/default behavior.

All configuration options from [Real Debrid](./real-debrid/) apply (rate limits, workers, proxy, etc.).

See [Configuration Reference](../configuration/#debrid-providers) for full options.

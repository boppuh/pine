# Tailscale Serve console ingress decision

Status: accepted for the Pine Research Console MVP.

## Decision

The Research Console uses Tailscale Serve as its identity-aware private ingress. Pine
accepts only the `Tailscale-User-Login` request header, normalizes the asserted login,
and requires an exact match in `PINE_CONSOLE_ALLOWED_IDENTITIES`.

The header is trusted only when Uvicorn reports both of these properties:

- no TCP client peer exists; and
- the local ASGI server address is the exact configured
  `PINE_CONSOLE_SOCKET_PATH` Unix socket.

A matching header delivered over loopback TCP, LAN, tailnet TCP, a test client, or any
other socket does not establish identity. Requests without an allowed claim fail with
403 before workflow data is loaded.

Tailscale Serve terminates HTTPS, removes caller-provided Tailscale identity headers,
and adds the authenticated user's login header for tailnet traffic. Funnel is not an
approved ingress because it is public and does not provide these identity headers.
This behavior is defined by the official
[Tailscale Serve identity-header contract](https://tailscale.com/docs/features/tailscale-serve#identity-headers).

## Deployment boundary

- The console listens only on its configured Unix socket. It has no TCP listener.
- Pine pre-binds the socket with mode `0600` in a non-group/world-writable runtime
  directory and passes the open descriptor to Uvicorn; Uvicorn never creates or
  widens the trusted socket path.
- The Serve configuration targets that socket and is not configured as Funnel.
- Socket ownership and mode permit only the console and ingress service boundary.
- Security-group rules expose neither the console nor the loopback Pine backend.
- Tailnet grants restrict the Serve URL to the approved operator before Pine's own
  exact identity allowlist is evaluated.

If the installed Tailscale release cannot proxy the permission-restricted Unix socket
under these conditions, deployment stops. Direct OIDC is the approved fallback; a
loopback TCP proxy with trusted identity headers is not.
